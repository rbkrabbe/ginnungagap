/// Regression tests that demonstrate the two crash-safety bugs in the shard
/// split implementation.
///
/// Both tests must **fail** (assert the buggy state) until the atomic-commit
/// fix is applied. After the fix they are updated to assert the correct state.
///
/// # Why `#[cfg(test)]` fault injection instead of the DST framework
///
/// The existing `sim_cluster` DST simulates crashes via `raft.shutdown()`, which
/// is a *graceful* shutdown that allows in-flight `apply()` calls to complete.
/// There is no way to interrupt a `spawn_blocking` thread mid-execution from
/// outside the thread, so the DST cannot land a kill signal between Phase 1's
/// `batch.commit()` and Phase 2's ShardMap writes — those two steps execute in
/// the same async task with no intervening yield that a shutdown signal would
/// win. The `arm_crash_after_phase1()` injection point is the standard Rust
/// technique for this class of test (used by TiKV, CockroachDB, etc.).
use std::sync::Arc;

use ggap_storage::fjall::{FjallStateMachine, FjallStore};
use ggap_storage::keys::{data_key, meta_key};
use ggap_storage::{ShardMap, StateMachineStore};
use ggap_types::{KeyRange, KvCommand, KvResponse, LogId};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn log_id(index: u64) -> LogId {
    LogId {
        term: 1,
        leader_id: 1,
        index,
    }
}

async fn apply_put(fsm: &FjallStateMachine, shard_id: u64, index: u64, key: &str, val: &str) {
    fsm.apply(
        shard_id,
        log_id(index),
        Some(KvCommand::Put {
            key: key.to_string(),
            value: val.as_bytes().to_vec(),
            ttl_ns: None,
            expect_version: 0,
        }),
        None,
    )
    .await
    .unwrap();
}

fn has_key(store: &FjallStore, shard_id: u64, key: &str) -> bool {
    store.data.get(data_key(shard_id, key)).unwrap().is_some()
}

fn last_applied_index(store: &FjallStore, shard_id: u64) -> Option<u64> {
    let raw = store
        .meta
        .get(meta_key(shard_id, "last_applied"))
        .unwrap()?;
    let (log_id, _): (LogId, _) =
        bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
    Some(log_id.index)
}

// ---------------------------------------------------------------------------
// Bug 1: non-atomic split loses data on crash between Phase 1 and Phase 2
// ---------------------------------------------------------------------------

/// Demonstrates that a crash between Phase 1 (data movement committed to disk)
/// and Phase 2 (ShardMap persisted) leaves the store in an unrecoverable
/// inconsistent state:
///
/// - Data for the upper half is deleted from shard 0 (Phase 1 ran)
/// - ShardMap still shows shard 0 owning the full range (Phase 2 never ran)
/// - `last_applied` is advanced past the split entry, so Raft won't re-apply it
/// - The upper-half data exists in shard 1's storage but is unroutable
#[tokio::test]
async fn bug1_nonatomic_split_loses_data_on_crash() {
    let dir = tempfile::tempdir().unwrap();

    // --- Setup: write some data then apply a split with crash injection ---
    {
        let store = FjallStore::open(dir.path()).unwrap();
        let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
        shard_map.initialize_default().await.unwrap();
        let (split_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut fsm = FjallStateMachine::new(store.clone());
        fsm.set_split_sender(split_tx);
        fsm.set_shard_map(shard_map.clone());

        apply_put(&fsm, 0, 1, "apple", "v1").await;
        apply_put(&fsm, 0, 2, "banana", "v2").await;
        apply_put(&fsm, 0, 3, "mango", "v3").await;
        apply_put(&fsm, 0, 4, "zebra", "v4").await;

        // Arm the fault injection: apply() will return Err after Phase 1
        // commits but before Phase 2 writes the ShardMap.
        fsm.arm_crash_after_phase1();

        let result = fsm
            .apply(
                0,
                log_id(5),
                Some(KvCommand::Split {
                    split_key: "m".into(),
                    new_shard_id: 1,
                    source_range: KeyRange {
                        start: String::new(),
                        end: String::new(),
                    },
                    source_members: [(1u64, "127.0.0.1:7001".to_string())].into(),
                }),
                None,
            )
            .await;

        assert!(
            result.is_err(),
            "expected simulated crash error, got: {:?}",
            result
        );
        // FSM is dropped here, simulating process death after Phase 1.
    }

    // --- Restart: reopen store from the same path ---
    // The fault injection fired after the atomic batch committed, so storage
    // is fully consistent: ShardMap, data, and last_applied were all written
    // in the same batch.commit() call.
    let store2 = FjallStore::open(dir.path()).unwrap();
    let shard_map2 = ShardMap::load(store2.clone()).unwrap();

    // Both shards are in the ShardMap — written atomically with the data.
    let shards = shard_map2.all_shards().await;
    assert_eq!(
        shards.len(),
        2,
        "both shards must be in the ShardMap after atomic commit"
    );

    // Lower-half data still in shard 0.
    assert!(
        has_key(&store2, 0, "apple"),
        "'apple' must remain in shard 0"
    );
    assert!(
        !has_key(&store2, 0, "mango"),
        "'mango' must have been moved out of shard 0"
    );

    // Upper-half data routable via shard 1.
    assert!(has_key(&store2, 1, "mango"), "'mango' must be in shard 1");

    // last_applied advanced, split will not be re-applied.
    assert_eq!(last_applied_index(&store2, 0), Some(5));
}

// ---------------------------------------------------------------------------
// Bug 2: no bootstrap_members stored for the new shard
// ---------------------------------------------------------------------------

/// Demonstrates that after a complete split (all phases), no membership info
/// is persisted for the new shard. On restart, `main.rs` falls back to
/// single-node initialisation for the new shard, silently discarding the
/// 3-node replication that the original cluster had.
#[tokio::test]
async fn bug2_no_bootstrap_members_for_split_shard() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = FjallStore::open(dir.path()).unwrap();
        let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
        shard_map.initialize_default().await.unwrap();
        let (split_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut fsm = FjallStateMachine::new(store.clone());
        fsm.set_split_sender(split_tx);
        fsm.set_shard_map(shard_map.clone());

        apply_put(&fsm, 0, 1, "apple", "v1").await;
        apply_put(&fsm, 0, 2, "mango", "v2").await;

        // Full split — no crash injection, all phases complete.
        let resp = fsm
            .apply(
                0,
                log_id(3),
                Some(KvCommand::Split {
                    split_key: "m".into(),
                    new_shard_id: 1,
                    source_range: KeyRange {
                        start: String::new(),
                        end: String::new(),
                    },
                    source_members: [(1u64, "127.0.0.1:7001".to_string())].into(),
                }),
                None,
            )
            .await
            .unwrap();

        assert!(
            matches!(resp, KvResponse::SplitComplete { new_shard_id: 1 }),
            "expected SplitComplete, got {:?}",
            resp
        );
    }

    // --- Restart ---
    let store2 = FjallStore::open(dir.path()).unwrap();
    let shard_map2 = ShardMap::load(store2.clone()).unwrap();

    // Split completed: both shards should be in the ShardMap.
    assert_eq!(
        shard_map2.all_shards().await.len(),
        2,
        "split completed, ShardMap must show 2 shards"
    );

    // bootstrap_members is now written atomically with the split data.
    // On restart, main.rs can read this key and initialise the new shard
    // with the correct multi-node membership.
    let bootstrap_key = meta_key(1, "bootstrap_members");
    let result = store2.meta.get(&bootstrap_key).unwrap();
    assert!(
        result.is_some(),
        "bootstrap_members must be present after the fix so restart uses correct membership"
    );

    // The stored membership should match what was passed in source_members.
    let raw = result.unwrap();
    let (members, _): (std::collections::BTreeMap<u64, String>, _) =
        bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
    assert_eq!(
        members.get(&1u64).map(|s| s.as_str()),
        Some("127.0.0.1:7001")
    );
}
