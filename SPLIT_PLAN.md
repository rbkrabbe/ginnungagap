# Fix Shard Split: Raft-Replicated Data Movement + Full Membership

## Context

`SplitCoordinator::do_split` has two problems:
1. **Data movement is local-only** — `build_partial_snapshot`, `install_partial_snapshot`, and `delete_range_from` run only on the leader node. Other nodes in the cluster never split their data.
2. **New shard initialized as single-node** — the new Raft group is created with only the local node, losing replication even though the source shard has 3 (or more) members.

The fix: propose a `Split` command through the source shard's Raft log so every node applies the data split deterministically, then bootstrap the new shard's Raft group with the source shard's membership.

## Design

### New command flow

```
Coordinator (leader)                    All nodes (via Raft apply)
─────────────────────                   ─────────────────────────
1. Validate split request
2. Mark source shard Splitting
3. Read source membership
4. Allocate new_shard_id
5. Propose KvCommand::Split ──────────► 6. apply(): copy keys >= split_key
   { split_key, new_shard_id }             to new_shard_id, delete from source
                                        7. Signal: "new shard ready"
                                        8. Background task creates Raft
                                           instance + registers in router
9. Wait for local Raft instance
10. Initialize with source membership
11. Update ShardMap (narrowed source +
    new shard ranges)
```

The write barrier is built-in: the Split command is ordered in the log after all prior writes.

### Raft group bootstrap on non-leader nodes

Each node needs a Raft instance for the new shard. Approach: **side-channel from apply**.

`GgapStateMachine` gets an `mpsc::UnboundedSender<SplitApplied>` channel. When it applies a Split command, it sends a `SplitApplied { new_shard_id }` event. A background task on each node receives this and creates the Raft instance + registers it in the router.

On the **leader** specifically, the coordinator waits for the local Raft instance to appear in the router, then calls `raft.initialize(source_membership)`. OpenRaft replicates to followers; if a follower's background task hasn't created its instance yet, the RPC returns "shard not found" and openraft retries automatically.

This is the same pattern CockroachDB and TiKV use for region splits — the split is a Raft command, and each node independently creates the new Raft group infrastructure as part of (or immediately after) applying it.

## Changes by file

### 1. `crates/ggap-types/src/lib.rs`

Add variant to `KvCommand`:
```rust
Split {
    split_key: String,
    new_shard_id: ShardId,
    source_range: KeyRange,
},
```

Add variant to `KvResponse`:
```rust
SplitComplete { new_shard_id: ShardId },
```

### 2. `crates/ggap-storage/src/fjall.rs` — `FjallStateMachine::apply()`

Add a `KvCommand::Split` arm. The current `apply()` runs entirely inside `spawn_blocking`, and the split helpers (`build_partial_snapshot` etc.) are async wrappers around their own `spawn_blocking`. To avoid nested spawn_blocking, extract the synchronous inner logic from each helper into a `fn(&FjallStore, ...)` and call those directly within the existing `spawn_blocking` block.

```rust
Some(KvCommand::Split { split_key, new_shard_id, source_range }) => {
    // Inside the existing spawn_blocking:
    // 1. Data movement (reuse extracted sync inner fns)
    let contents = build_partial_snapshot_sync(&store, shard_id, &split_key)?;
    install_partial_snapshot_sync(&store, new_shard_id, &contents)?;
    delete_range_from_sync(&store, shard_id, &split_key)?;

    // 2. Update ShardMap: narrow source, add new shard
    //    (deterministic on all nodes — no external reads)
    let updated_source = ShardInfo {
        shard_id,
        range: KeyRange { start: source_range.start, end: split_key.clone() },
        state: ShardState::Active,
    };
    let new_shard = ShardInfo {
        shard_id: new_shard_id,
        range: KeyRange { start: split_key, end: source_range.end },
        state: ShardState::Active,
    };
    // ShardMap needs a sync put method (or batch write to meta keyspace directly)

    // 3. Signal split_tx channel (must be done outside spawn_blocking)
    return KvResponse::SplitComplete { new_shard_id };
}
```

**ShardMap update in apply**: `ShardMap` currently has async methods. For the sync `spawn_blocking` context, either:
- Add `put_shard_sync(&self, info)` that writes directly to fjall (no async)
- Or handle the ShardMap update outside `spawn_blocking` after the data movement returns

The split_tx channel send also needs to happen outside `spawn_blocking` (tokio channels aren't usable in blocking context). So the Split arm should likely be handled as a separate code path that runs data movement in `spawn_blocking`, then does ShardMap + channel signaling in async context afterward.

### 3. `crates/ggap-storage/src/fjall.rs` — Split signal channel

Add to `FjallStateMachine`:
```rust
split_tx: Option<tokio::sync::mpsc::UnboundedSender<SplitApplied>>,
```

New struct:
```rust
pub struct SplitApplied {
    pub new_shard_id: ShardId,
}
```

Add setter method:
```rust
pub fn set_split_sender(&mut self, tx: mpsc::UnboundedSender<SplitApplied>) {
    self.split_tx = Some(tx);
}
```

When the Split command is applied, send the event through the channel.

### 4. `crates/ggap-consensus/src/state_machine.rs` — `GgapStateMachine::apply()`

The `EntryPayload::Normal(cmd)` arm already passes through to `fsm.apply()`. No change needed here — the new `KvCommand::Split` variant flows through naturally.

### 5. `crates/ggap-consensus/src/split.rs` — `SplitCoordinator`

Refactor `do_split()`:

```
1. Get source node from router
2. Read source shard info (for range) from shard_map
3. Read source membership from source_node.raft()
   (openraft 0.9: raft.with_raft_state(|st| st.membership_state...) or
    check metrics/get_membership API)
4. Allocate new_shard_id via shard_map.next_shard_id()
5. Propose KvCommand::Split { split_key, new_shard_id, source_range }
   via source_node.raft().client_write(cmd)
   — This replaces: ensure_linearizable + build_partial_snapshot +
     install_partial_snapshot + delete_range_from + ShardMap updates
   — ShardMap is now updated inside apply on all nodes
6. Wait for new shard's Raft instance to appear in router
   (poll router.get_node(new_shard_id) with timeout)
7. Initialize new shard's Raft with source membership:
   new_raft.initialize(source_members)
8. Wait for new shard leader election
```

Remove: all local Raft creation code (lines 192-226) — moves to background split handler.
Remove: ShardMap updates from coordinator — moves to apply path.
Remove: `ensure_linearizable()` — the Split command IS the barrier.

### 6. `crates/ggap-consensus/src/node.rs` or new file — Background split handler

New async task that runs on every node:
```rust
pub async fn run_split_handler(
    mut rx: mpsc::UnboundedReceiver<SplitApplied>,
    store: Arc<FjallStore>,
    fsm: Arc<FjallStateMachine>,
    router: Arc<ShardRouter>,
    node_id: u64,
    cluster_addr: String,
    raft_config: Arc<openraft::Config>,
) {
    while let Some(event) = rx.recv().await {
        let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), event.new_shard_id);
        let sm = GgapStateMachine::new(fsm.clone(), event.new_shard_id);
        let net = GgapNetworkFactory::new(event.new_shard_id);
        let raft = Arc::new(GgapRaft::new(node_id, raft_config.clone(), net, log_store, sm).await.unwrap());
        let node = Arc::new(OpenRaftNode::new(raft.clone(), fsm.clone(), event.new_shard_id, node_id, ...));
        let cluster = Arc::new(OpenRaftCluster::new(raft));
        router.add_shard(event.new_shard_id, node, cluster).await;
    }
}
```

Spawned in `ggap-node/src/main.rs` alongside the gRPC servers.

### 7. `crates/ggap-node/src/main.rs`

- Create the `mpsc::unbounded_channel()` for split events
- Pass the sender to `FjallStateMachine` via `set_split_sender()`
- Spawn `run_split_handler` task
- Pass the necessary context (store, fsm, router, node_id, cluster_addr, raft_config)

### 8. `crates/ggap-server/src/kv_service.rs`

The `unreachable!()` guards for unexpected `KvResponse` variants in Put/Delete/CAS handlers don't need changes — `SplitComplete` will only be returned to the coordinator's `client_write` call, not through the KV service path.

However, if a `Split` command somehow arrives through the normal KV propose path, it would return `SplitComplete` which hits `unreachable!()`. This is fine — the split is only proposed by the coordinator.

## Design decisions (confirmed)

- **Raft bootstrap on followers**: Side-channel from apply. `FjallStateMachine` sends `SplitApplied` events through an mpsc channel. Background task on each node creates the Raft instance. Leader calls `initialize()` after its local instance is ready; openraft retries handle any follower lag.
- **ShardMap sync**: Updated deterministically in apply on all nodes. The `KvCommand::Split` carries `source_range: KeyRange` so each node can compute both new ranges without reading ShardMap (which would be non-deterministic if out of sync).

## Verification

1. **Unit test**: Extend existing `FjallStateMachine` tests to apply a `KvCommand::Split` and verify data is correctly split across shard_ids
2. **Integration test**: Extend `three_node_cluster.rs`:
   - Write keys spanning the alphabet
   - Call `split_shard` via admin gRPC
   - Verify all keys are still readable (some from source shard, some from new shard)
   - Verify both shards have 3 members
   - Kill a node and verify both shards remain available
3. **DST**: Extend `sim_cluster.rs` with a split-under-load scenario
