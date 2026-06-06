import React from "react";
import { createPromiseClient } from "@connectrpc/connect";
import { AdminService } from "../gen/ginnungagap/v1/cluster_connect";
import { clusterTransport } from "../transport";
import type { ShardInfoProto, ClusterStatusResponse } from "../gen/ginnungagap/v1/cluster_pb";

const client = createPromiseClient(AdminService, clusterTransport);

const RUNES = { membership: "ᛚ" };

function fmtNum(n: number) {
  return n.toLocaleString();
}

function RolePill({ role }: { role: string }) {
  if (role === "leader") return <span className="pill spark"><span className="pdot" />leader</span>;
  if (role === "follower") return <span className="pill frost"><span className="pdot" />follower</span>;
  if (role === "learner") return <span className="pill"><span className="pdot" />learner</span>;
  if (role === "joining") return <span className="pill warn"><span className="pdot" />joining</span>;
  return null;
}

export default function MembershipScreen({ toast }: { toast: (t: { kind?: string; text: string; sub?: string }) => void }) {
  const [shards, setShards] = React.useState<ShardInfoProto[]>([]);
  const [selectedShardId, setSelectedShardId] = React.useState<bigint>(0n);
  const [status, setStatus] = React.useState<ClusterStatusResponse | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  const [addOpen, setAddOpen] = React.useState(false);
  const [addNodeId, setAddNodeId] = React.useState("6");
  const [addClientAddr, setAddClientAddr] = React.useState("10.42.0.16:17000");
  const [addClusterAddr, setAddClusterAddr] = React.useState("10.42.0.16:17001");
  const [adding, setAdding] = React.useState(false);

  const [changeOpen, setChangeOpen] = React.useState(false);
  const [proposedIds, setProposedIds] = React.useState("");
  const [changing, setChanging] = React.useState(false);

  const loadShards = React.useCallback(async () => {
    try {
      const resp = await client.listShards({});
      setShards(resp.shards);
      if (resp.shards.length > 0) {
        setSelectedShardId((prev) => (resp.shards.find((s) => s.shardId === prev) ? prev : resp.shards[0].shardId));
      }
    } catch (e) {
      setError(String(e));
    }
  }, []);

  const loadStatus = React.useCallback(async (shardId: bigint) => {
    setLoading(true);
    setError(null);
    try {
      const resp = await client.clusterStatus({ shardId });
      setStatus(resp);
    } catch (e) {
      setError(String(e));
      setStatus(null);
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    loadShards();
  }, []);

  React.useEffect(() => {
    loadStatus(selectedShardId);
  }, [selectedShardId]);

  // Build voter/learner lists from status
  const voters = React.useMemo(() => status?.nodes ?? [], [status]);
  const learners = React.useMemo(() => status?.learners ?? [], [status]);
  const voterIds = voters.map((n) => n.nodeId);
  const quorum = Math.floor(voters.length / 2) + 1;
  const noData = status?.lastObservedAgeMs === undefined;

  async function doAddLearner() {
    setAdding(true);
    try {
      const resp = await client.addLearner({
        node: { nodeId: BigInt(addNodeId), clientAddr: addClientAddr, clusterAddr: addClusterAddr },
        shardId: selectedShardId,
      });
      if (resp.ok) {
        toast({ kind: "ok", text: `node-${addNodeId} added as learner`, sub: `AdminService/AddLearner · shard ${selectedShardId} · OK` });
        setAddOpen(false);
        await loadStatus(selectedShardId);
      } else {
        toast({ kind: "err", text: "AddLearner failed", sub: resp.error });
      }
    } catch (e) {
      toast({ kind: "err", text: "AddLearner error", sub: String(e) });
    } finally {
      setAdding(false);
    }
  }

  async function doChangeMembership() {
    setChanging(true);
    try {
      const ids = proposedIds
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .map(BigInt);
      const resp = await client.changeMembership({ nodeIds: ids, shardId: selectedShardId });
      if (resp.ok) {
        toast({ kind: "ok", text: "Membership changed", sub: `AdminService/ChangeMembership · shard ${selectedShardId} · OK` });
        setChangeOpen(false);
        await loadStatus(selectedShardId);
      } else {
        toast({ kind: "err", text: "ChangeMembership failed", sub: resp.error });
      }
    } catch (e) {
      toast({ kind: "err", text: "ChangeMembership error", sub: String(e) });
    } finally {
      setChanging(false);
    }
  }

  if (error) {
    return (
      <div className="fade-in" style={{ padding: 40 }}>
        <div style={{ color: "var(--err)", marginBottom: 12 }}>Failed to load: {error}</div>
        <button className="btn btn-sm" onClick={() => loadStatus(selectedShardId)}>Retry</button>
      </div>
    );
  }

  return (
    <div className="fade-in">
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="glyph">{RUNES.membership}</span>
            Membership
          </h1>
          <p className="page-sub">
            {noData ? (
              "Shard membership — no snapshot yet"
            ) : (
              <>
                {voters.length} voter{voters.length !== 1 ? "s" : ""} · {learners.length} learner{learners.length !== 1 ? "s" : ""} · quorum{" "}
                <span className="mono">{quorum}</span>
              </>
            )}
          </p>
        </div>
        <div className="page-actions">
          {/* Shard selector */}
          {shards.length > 0 && (
            <select
              className="field"
              value={String(selectedShardId)}
              onChange={(e) => setSelectedShardId(BigInt(e.target.value))}
              style={{ background: "var(--bg-sunken)", border: "1px solid var(--line)", borderRadius: "var(--radius)", padding: "4px 8px", color: "var(--fg)", fontFamily: "var(--font-mono)", fontSize: 12 }}
            >
              {shards.map((s) => (
                <option key={String(s.shardId)} value={String(s.shardId)}>
                  shard {String(s.shardId)} · "{s.rangeStart || "—∞"}" → "{s.rangeEnd || "+∞"}"
                </option>
              ))}
            </select>
          )}
          <button className="btn btn-sm" onClick={() => setChangeOpen(true)}>ChangeMembership…</button>
          <button className="btn btn-primary btn-sm" onClick={() => setAddOpen(true)}>+ Add learner</button>
        </div>
      </div>

      {loading && !status ? (
        <div style={{ padding: 40, color: "var(--fg-3)" }}><span className="spinner" /> Loading…</div>
      ) : (
        <>
          {noData && (
            <div className="panel" style={{ padding: 20, marginBottom: 18, color: "var(--fg-3)" }}>
              No consensus snapshot observed yet for shard {String(selectedShardId)}. Membership will appear once the shard's Raft group is initialized.
            </div>
          )}

          <div className="grid-2">
            {/* Voters panel */}
            <div className="panel">
              <div className="panel-head">
                <div className="panel-title">Voters · {voters.length}</div>
                {!noData && <span className="muted mono" style={{ fontSize: 11 }}>quorum {quorum}/{voters.length}</span>}
              </div>
              <table className="tbl">
                <thead>
                  <tr>
                    <th>node</th>
                    <th>role</th>
                    <th>cluster_addr</th>
                    <th className="num">last_applied</th>
                  </tr>
                </thead>
                <tbody>
                  {voters.map((n) => {
                    const isLeader = status?.leaderId === n.nodeId;
                    return (
                      <tr key={String(n.nodeId)}>
                        <td className="mono"><strong>node-{String(n.nodeId)}</strong></td>
                        <td><RolePill role={isLeader ? "leader" : "follower"} /></td>
                        <td className="mono" style={{ color: "var(--fg-3)" }}>
                          {n.clusterAddr || <span className="dim" title="cluster_addr not available for remote nodes">—</span>}
                        </td>
                        <td className="num">{fmtNum(Number(status?.lastApplied ?? 0))}</td>
                      </tr>
                    );
                  })}
                  {voters.length === 0 && (
                    <tr><td colSpan={4} className="muted" style={{ textAlign: "center", padding: 20 }}>No voters in snapshot.</td></tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Learners panel */}
            <div className="panel">
              <div className="panel-head">
                <div className="panel-title">Learners · {learners.length}</div>
                <span className="muted mono" style={{ fontSize: 11 }}>not part of quorum</span>
              </div>
              <table className="tbl">
                <thead>
                  <tr>
                    <th>node</th>
                    <th>role</th>
                    <th>cluster_addr</th>
                    <th className="num">lag</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {learners.map((n) => (
                    <tr key={String(n.nodeId)}>
                      <td className="mono"><strong>node-{String(n.nodeId)}</strong></td>
                      <td><RolePill role="learner" /></td>
                      <td className="mono" style={{ color: "var(--fg-3)" }}>
                        {n.clusterAddr || <span className="dim">—</span>}
                      </td>
                      <td className="num dim" title="lag_ms unavailable — no host telemetry">—</td>
                      <td />
                    </tr>
                  ))}
                  {learners.length === 0 && (
                    <tr>
                      <td colSpan={5} className="muted" style={{ textAlign: "center", padding: 20 }}>
                        No learners. Add one to grow the cluster.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      <div style={{ height: 18 }} />

      {/* Protocol explainer */}
      <div className="panel">
        <div className="panel-head">
          <div className="panel-title">Membership change protocol · shard {String(selectedShardId)}</div>
          <span className="muted mono" style={{ fontSize: 11 }}>AdminService · gRPC-web</span>
        </div>
        <div className="panel-body" style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 14 }}>
          {[
            { rune: "ᚹ", title: "1. AddLearner", body: "Registers a non-voting replica. Receives Raft log replication but cannot vote in elections.", code: `AddLearnerRequest { node: NodeInfo { … }, shard_id: ${selectedShardId} }` },
            { rune: "ᛟ", title: "2. Catch up", body: "Learner streams entries from the leader until last_applied is within tolerance. Lag drops to ~ms.", code: "InstallSnapshot · AppendEntries" },
            { rune: "ᛚ", title: "3. ChangeMembership", body: "Joint-consensus reconfiguration promotes the learner into the voter set, raising quorum size.", code: `ChangeMembershipRequest { node_ids: [${voterIds.join(",")}], shard_id: ${selectedShardId} }` },
          ].map((s, i) => (
            <div key={i} style={{ background: "var(--bg-sunken)", border: "1px solid var(--line-soft)", borderRadius: 8, padding: 14 }}>
              <div className="hstack" style={{ marginBottom: 6 }}>
                <span className="rune" style={{ color: "var(--spark)", fontSize: 18 }}>{s.rune}</span>
                <strong style={{ fontSize: 13 }}>{s.title}</strong>
              </div>
              <p style={{ margin: "4px 0 10px", fontSize: 12.5, color: "var(--fg-2)", lineHeight: 1.5 }}>{s.body}</p>
              <div className="mono" style={{ fontSize: 10.5, color: "var(--fg-3)", padding: "6px 8px", background: "var(--bg-void)", borderRadius: 4, border: "1px solid var(--line-soft)" }}>{s.code}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Add learner modal */}
      {addOpen && (
        <div className="modal-veil" onClick={(e) => { if (e.target === e.currentTarget && !adding) setAddOpen(false); }}>
          <div className="modal">
            <div className="modal-head">
              <div className="modal-title"><span className="rune" style={{ color: "var(--spark)", marginRight: 8 }}>ᚹ</span>Add learner · shard {String(selectedShardId)}</div>
              {!adding && <button className="btn btn-ghost btn-sm" onClick={() => setAddOpen(false)}>Cancel</button>}
            </div>
            <div className="modal-body">
              <p className="muted" style={{ marginTop: 0, fontSize: 12.5 }}>
                Registers a new replica as a learner on shard {String(selectedShardId)}. The node must already be running and reachable on its cluster_addr.
              </p>
              <div className="field"><label>node_id</label><input type="number" value={addNodeId} onChange={(e) => setAddNodeId(e.target.value)} disabled={adding} /></div>
              <div className="field"><label>client_addr</label><input value={addClientAddr} onChange={(e) => setAddClientAddr(e.target.value)} disabled={adding} /></div>
              <div className="field"><label>cluster_addr</label><input value={addClusterAddr} onChange={(e) => setAddClusterAddr(e.target.value)} disabled={adding} /></div>
            </div>
            <div className="modal-foot">
              {!adding && <button className="btn btn-ghost btn-sm" onClick={() => setAddOpen(false)}>Cancel</button>}
              <button className="btn btn-primary btn-sm" onClick={doAddLearner} disabled={adding}>
                {adding ? <><span className="spinner" /> registering…</> : "AddLearner →"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ChangeMembership modal */}
      {changeOpen && (
        <div className="modal-veil" onClick={(e) => { if (e.target === e.currentTarget && !changing) setChangeOpen(false); }}>
          <div className="modal">
            <div className="modal-head">
              <div className="modal-title"><span className="rune" style={{ color: "var(--spark)", marginRight: 8 }}>ᛚ</span>ChangeMembership · shard {String(selectedShardId)}</div>
              {!changing && <button className="btn btn-ghost btn-sm" onClick={() => setChangeOpen(false)}>Close</button>}
            </div>
            <div className="modal-body">
              <p className="muted" style={{ marginTop: 0, fontSize: 12.5 }}>
                Replaces the voting set in a single joint-consensus step for shard {String(selectedShardId)}.
              </p>
              <div className="field">
                <label>current voters</label>
                <input value={`[${voterIds.join(", ")}]`} disabled />
              </div>
              <div className="field">
                <label>proposed voter node_ids (comma-separated)</label>
                <input
                  value={proposedIds || voterIds.join(", ")}
                  onChange={(e) => setProposedIds(e.target.value)}
                  disabled={changing}
                  placeholder={voterIds.join(", ")}
                />
              </div>
            </div>
            <div className="modal-foot">
              {!changing && <button className="btn btn-ghost btn-sm" onClick={() => setChangeOpen(false)}>Cancel</button>}
              <button className="btn btn-primary btn-sm" onClick={doChangeMembership} disabled={changing}>
                {changing ? <><span className="spinner" /> applying…</> : "Apply →"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
