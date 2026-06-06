import React from "react";
import { createPromiseClient } from "@connectrpc/connect";
import { AdminService } from "../gen/ginnungagap/v1/cluster_connect";
import { clusterTransport } from "../transport";
import type { ShardInfoProto } from "../gen/ginnungagap/v1/cluster_pb";

const client = createPromiseClient(AdminService, clusterTransport);

const RUNES = { nodes: "ᚾ" };

function fmtNum(n: number) {
  return n.toLocaleString();
}

// Unavailable field placeholder
function NA({ title }: { title: string }) {
  return (
    <span className="dim" title={title}>
      —
    </span>
  );
}

type NodeRole = "leader" | "follower" | "learner" | "absent";

interface NodeEntry {
  nodeId: bigint;
  clusterAddr?: string;
  clientAddr?: string;
}

interface NodeRoleCounts {
  leader: bigint[];
  follower: bigint[];
  learner: bigint[];
  absent: bigint[];
}

function deriveNodes(shards: ShardInfoProto[]): NodeEntry[] {
  const seen = new Map<string, NodeEntry>();
  for (const s of shards) {
    const all = [
      ...(s.leaderId !== undefined ? [s.leaderId] : []),
      ...s.voters,
      ...s.learners,
    ];
    for (const id of all) {
      const key = String(id);
      if (!seen.has(key)) seen.set(key, { nodeId: id });
    }
  }
  return [...seen.values()].sort((a, b) => (a.nodeId < b.nodeId ? -1 : 1));
}

function nodeRoleCounts(nodeId: bigint, shards: ShardInfoProto[]): NodeRoleCounts {
  const result: NodeRoleCounts = { leader: [], follower: [], learner: [], absent: [] };
  for (const s of shards) {
    let role: NodeRole = "absent";
    if (s.leaderId === nodeId) role = "leader";
    else if (s.voters.includes(nodeId)) role = "follower";
    else if (s.learners.includes(nodeId)) role = "learner";
    result[role].push(s.shardId);
  }
  return result;
}

function NodeDistBar({ counts, total }: { counts: NodeRoleCounts; total: number }) {
  const segs = [
    { k: "leader", v: counts.leader.length, color: "var(--spark)" },
    { k: "follower", v: counts.follower.length, color: "var(--frost)" },
    { k: "learner", v: counts.learner.length, color: "oklch(0.55 0.04 258)" },
    { k: "absent", v: counts.absent.length, color: "var(--bg-sunken)" },
  ];
  return (
    <div style={{ display: "flex", height: 8, borderRadius: 4, overflow: "hidden", border: "1px solid var(--line-soft)" }}>
      {segs.map((s) =>
        s.v > 0 ? <div key={s.k} title={`${s.k}: ${s.v}`} style={{ flex: s.v / total, background: s.color }} /> : null,
      )}
    </div>
  );
}

function FreshnessBadge({ ageMs }: { ageMs: bigint | undefined }) {
  if (ageMs === undefined) return <span className="pill">no snapshot</span>;
  if (ageMs === 0n) return <span className="pill ok"><span className="pdot" />live</span>;
  return <span className="pill warn"><span className="pdot" />{(Number(ageMs) / 1000).toFixed(1)}s ago</span>;
}

export default function NodesScreen({ toast: _toast }: { toast: (t: { kind?: string; text: string; sub?: string }) => void }) {
  const [shards, setShards] = React.useState<ShardInfoProto[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = React.useState<bigint | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await client.listShards({});
      setShards(resp.shards);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, []);

  const nodes = React.useMemo(() => deriveNodes(shards), [shards]);
  const selectedNode = nodes.find((n) => n.nodeId === selectedNodeId) ?? nodes[0] ?? null;

  React.useEffect(() => {
    if (selectedNodeId === null && nodes.length > 0) setSelectedNodeId(nodes[0].nodeId);
  }, [nodes, selectedNodeId]);

  const roleCounts = React.useMemo(() => {
    const out = new Map<string, NodeRoleCounts>();
    for (const n of nodes) out.set(String(n.nodeId), nodeRoleCounts(n.nodeId, shards));
    return out;
  }, [nodes, shards]);

  if (loading && shards.length === 0) {
    return (
      <div className="fade-in" style={{ padding: 40, color: "var(--fg-3)" }}>
        <span className="spinner" /> Loading nodes…
      </div>
    );
  }

  if (error) {
    return (
      <div className="fade-in" style={{ padding: 40 }}>
        <div style={{ color: "var(--err)", marginBottom: 12 }}>Failed to load: {error}</div>
        <button className="btn btn-sm" onClick={load}>Retry</button>
      </div>
    );
  }

  const selectedCounts = selectedNode ? roleCounts.get(String(selectedNode.nodeId)) : null;

  return (
    <div className="fade-in">
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="glyph">{RUNES.nodes}</span>
            Nodes
          </h1>
          <p className="page-sub">
            Multi-raft cluster · {nodes.length} node{nodes.length !== 1 ? "s" : ""} · {shards.length} independent Raft groups
            <span className="muted"> — each node has a role per shard</span>
          </p>
        </div>
        <div className="page-actions">
          <button className="btn btn-ghost btn-sm" onClick={load}>Refresh</button>
        </div>
      </div>

      <div className="grid-3">
        {/* Cluster member table */}
        <div className="panel">
          <div className="panel-head">
            <div className="panel-title">Cluster members</div>
            <span className="muted mono" style={{ fontSize: 11 }}>roles vary per shard</span>
          </div>
          <table className="tbl">
            <thead>
              <tr>
                <th>node</th>
                <th>cluster_addr</th>
                <th>region</th>
                <th className="num">leader</th>
                <th className="num">follower</th>
                <th className="num">learner</th>
                <th style={{ width: 130 }}>distribution</th>
                <th>uptime</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => {
                const c = roleCounts.get(String(n.nodeId));
                if (!c) return null;
                return (
                  <tr
                    key={String(n.nodeId)}
                    className={selectedNode?.nodeId === n.nodeId ? "selected" : ""}
                    onClick={() => setSelectedNodeId(n.nodeId)}
                    style={{ cursor: "pointer" }}
                  >
                    <td className="mono"><strong>node-{String(n.nodeId)}</strong></td>
                    <td className="mono" style={{ color: "var(--fg-3)" }}>
                      {n.clusterAddr ?? <NA title="cluster_addr not exposed over gossip for remote nodes" />}
                    </td>
                    <td><NA title="host telemetry unavailable — no backend yet" /></td>
                    <td className="num" style={{ color: c.leader.length > 0 ? "var(--spark)" : "var(--fg-4)" }}>{c.leader.length}</td>
                    <td className="num" style={{ color: c.follower.length > 0 ? "var(--frost)" : "var(--fg-4)" }}>{c.follower.length}</td>
                    <td className="num" style={{ color: c.learner.length > 0 ? "var(--fg-2)" : "var(--fg-4)" }}>{c.learner.length}</td>
                    <td><NodeDistBar counts={c} total={Math.max(shards.length, 1)} /></td>
                    <td><NA title="host telemetry unavailable" /></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="panel-foot">
            <span className="hstack" style={{ fontSize: 11 }}>
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}><span style={{ width: 8, height: 8, background: "var(--spark)", borderRadius: 2 }} />leader</span>
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}><span style={{ width: 8, height: 8, background: "var(--frost)", borderRadius: 2 }} />follower</span>
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}><span style={{ width: 8, height: 8, background: "oklch(0.55 0.04 258)", borderRadius: 2 }} />learner</span>
            </span>
            <span style={{ flex: 1 }} />
            <span className="mono">click a row to inspect</span>
          </div>
        </div>

        {/* Node detail */}
        {selectedNode && selectedCounts && (
          <div className="panel">
            <div className="panel-head">
              <div className="panel-title">node-{String(selectedNode.nodeId)} detail</div>
              <span className="hstack" style={{ gap: 4 }}>
                {selectedCounts.leader.length > 0 && <span className="pill spark"><span className="pdot" />{selectedCounts.leader.length} leader</span>}
                {selectedCounts.follower.length > 0 && <span className="pill frost"><span className="pdot" />{selectedCounts.follower.length} follower</span>}
                {selectedCounts.learner.length > 0 && <span className="pill"><span className="pdot" />{selectedCounts.learner.length} learner</span>}
              </span>
            </div>
            <div className="panel-body">
              <dl className="kvp">
                <dt>node_id</dt><dd>{String(selectedNode.nodeId)}</dd>
                <dt>client_addr</dt><dd><NA title="client_addr not included in shard membership data" /></dd>
                <dt>cluster_addr</dt><dd>{selectedNode.clusterAddr ?? <NA title="unavailable for non-local nodes" />}</dd>
                <dt>region</dt><dd><NA title="host telemetry unavailable" /></dd>
                <dt>version</dt><dd><NA title="host telemetry unavailable" /></dd>
                <dt>uptime</dt><dd><NA title="host telemetry unavailable" /></dd>
                <dt>cpu</dt><dd><NA title="host telemetry unavailable" /></dd>
                <dt>mem</dt><dd><NA title="host telemetry unavailable" /></dd>
                <dt>disk</dt><dd><NA title="host telemetry unavailable" /></dd>
              </dl>
            </div>
          </div>
        )}
      </div>

      <div style={{ height: 18 }} />

      {/* Per-shard role table for selected node */}
      {selectedNode && selectedCounts && (
        <div className="panel">
          <div className="panel-head">
            <div className="panel-title">node-{String(selectedNode.nodeId)} · per-shard role</div>
            <span className="muted mono" style={{ fontSize: 11 }}>
              leader for {selectedCounts.leader.length} · follower for {selectedCounts.follower.length} · learner for {selectedCounts.learner.length} · absent from {selectedCounts.absent.length}
            </span>
          </div>
          <div className="panel-body">
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 14 }}>
              {(["leader", "follower", "learner"] as const).map((kind) => (
                <div key={kind} style={{ background: "var(--bg-sunken)", border: "1px solid var(--line-soft)", borderRadius: 8, padding: 12, minHeight: 100 }}>
                  <div className="spread" style={{ marginBottom: 8 }}>
                    <span style={{ fontSize: 11, letterSpacing: "0.04em", textTransform: "uppercase", color: "var(--fg-3)" }}>
                      {kind === "leader" ? "Leader for" : kind === "follower" ? "Follower for" : "Learner for"}
                    </span>
                    <span className="mono tnum" style={{ fontSize: 13, color: kind === "leader" ? "var(--spark)" : kind === "follower" ? "var(--frost)" : "var(--fg-2)" }}>
                      {selectedCounts[kind].length}
                    </span>
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                    {selectedCounts[kind].length === 0 ? (
                      <span className="muted" style={{ fontSize: 11.5 }}>—</span>
                    ) : selectedCounts[kind].map((sid) => (
                      <span key={String(sid)} className="mono" style={{
                        display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 7px", fontSize: 11,
                        borderRadius: 4, border: kind === "leader" ? "1px solid oklch(from var(--spark) l c h / 0.5)" : "1px solid var(--line-soft)",
                        background: kind === "leader" ? "oklch(from var(--spark) l c h / 0.10)" : "var(--bg-sunken)",
                        color: kind === "leader" ? "var(--spark)" : kind === "follower" ? "var(--frost)" : "var(--fg-2)",
                      }}>
                        <span style={{ width: 5, height: 5, borderRadius: "50%", background: kind === "leader" ? "var(--spark)" : kind === "follower" ? "var(--frost)" : "var(--fg-3)" }} />
                        {String(sid)}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            <div className="divider" />

            <table className="tbl">
              <thead>
                <tr>
                  <th style={{ width: 60 }}>shard</th>
                  <th>range</th>
                  <th>role on node-{String(selectedNode.nodeId)}</th>
                  <th>shard leader</th>
                  <th>peers (voters)</th>
                  <th>term</th>
                  <th>applied</th>
                  <th>freshness</th>
                  <th className="num">keys</th>
                  <th className="num">qps</th>
                </tr>
              </thead>
              <tbody>
                {shards.map((s) => {
                  let kind: "spark" | "frost" | "" = "";
                  let label = "—";
                  if (s.leaderId === selectedNode.nodeId) { kind = "spark"; label = "leader"; }
                  else if (s.voters.includes(selectedNode.nodeId)) { kind = "frost"; label = "follower"; }
                  else if (s.learners.includes(selectedNode.nodeId)) { kind = ""; label = "learner"; }

                  const noData = s.lastObservedAgeMs === undefined;
                  return (
                    <tr key={String(s.shardId)}>
                      <td className="mono"><strong>{String(s.shardId)}</strong></td>
                      <td className="mono" style={{ color: "var(--fg-3)", fontSize: 11 }}>"{s.rangeStart || "—∞"}" → "{s.rangeEnd || "+∞"}"</td>
                      <td>
                        {label === "—"
                          ? <span className="muted mono" style={{ fontSize: 11 }}>not assigned</span>
                          : <span className={`pill ${kind}`}><span className="pdot" />{label}</span>}
                      </td>
                      <td className="mono" style={{ color: s.leaderId === selectedNode.nodeId ? "var(--spark)" : "var(--fg-2)" }}>
                        {noData ? <span className="dim">—</span> : s.leaderId !== undefined ? `node-${s.leaderId}` : <span className="dim">none</span>}
                        {s.leaderId === selectedNode.nodeId && " (this)"}
                      </td>
                      <td className="mono" style={{ color: "var(--fg-3)", fontSize: 11 }}>
                        {s.voters.filter((v) => v !== selectedNode.nodeId).map((v) => `node-${v}`).join(", ") || "—"}
                        {s.learners.length > 0 && <span style={{ color: "var(--fg-4)" }}> · learners: {s.learners.map((l) => `node-${l}`).join(", ")}</span>}
                      </td>
                      <td className="mono">{noData ? <span className="dim">—</span> : String(s.term)}</td>
                      <td className="mono">{noData ? <span className="dim">—</span> : fmtNum(Number(s.lastApplied))}</td>
                      <td><FreshnessBadge ageMs={s.lastObservedAgeMs} /></td>
                      <td className="num dim" title="unavailable">—</td>
                      <td className="num dim" title="unavailable">—</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
