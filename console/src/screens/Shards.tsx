import React from "react";
import { createPromiseClient } from "@connectrpc/connect";
import { AdminService } from "../gen/ginnungagap/v1/cluster_connect";
import { clusterTransport } from "../transport";
import type { ShardInfoProto } from "../gen/ginnungagap/v1/cluster_pb";

const client = createPromiseClient(AdminService, clusterTransport);

const RUNES = { shards: "ᛊ" };

function fmtNum(n: number) {
  return n.toLocaleString();
}

function be_u64_hex(n: bigint): string {
  const hex = n.toString(16).padStart(16, "0");
  return "0x" + (hex.match(/.{2}/g) ?? []).join(" ");
}

function colorFor(s: ShardInfoProto): string {
  if (s.state === "splitting") return "oklch(0.78 0.14 65)";
  if (s.state === "under-replicated") return "oklch(0.72 0.16 25)";
  const hue = 220 + (Number(s.shardId) * 13) % 120;
  return `oklch(0.62 0.08 ${hue})`;
}

function FreshnessBadge({ ageMs }: { ageMs: bigint | undefined }) {
  if (ageMs === undefined) {
    return (
      <span className="pill" title="No consensus snapshot observed yet — fields below are absent-of-data, not a real leaderless shard">
        no snapshot
      </span>
    );
  }
  if (ageMs === 0n) {
    return (
      <span className="pill ok" title="Hosted locally — live data">
        <span className="pdot" />
        live
      </span>
    );
  }
  const secs = (Number(ageMs) / 1000).toFixed(1);
  return (
    <span className="pill warn" title={`Learned via gossip ${secs}s ago — may be stale`}>
      <span className="pdot" />
      {secs}s ago
    </span>
  );
}

const SPLIT_STEPS = [
  "Validate request",
  "Mark source shard Splitting",
  "Write barrier (linearizable no-op)",
  "Build partial snapshot keys ≥ split_key",
  "Allocate new shard ID",
  "Install partial snapshot on new shard",
  "Delete transferred keys from source",
  "Update ShardMap",
  "Bootstrap Raft group · resume writes",
];

export default function ShardsScreen({ toast }: { toast: (t: { kind?: string; text: string; sub?: string }) => void }) {
  const [shards, setShards] = React.useState<ShardInfoProto[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [selectedId, setSelectedId] = React.useState<bigint | null>(null);

  const [splitOpen, setSplitOpen] = React.useState(false);
  const [splitFor, setSplitFor] = React.useState<ShardInfoProto | null>(null);
  const [splitKey, setSplitKey] = React.useState("");
  const [splitStep, setSplitStep] = React.useState(0);
  const [splitting, setSplitting] = React.useState(false);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await client.listShards({});
      setShards(resp.shards);
      if (resp.shards.length > 0 && selectedId === null) {
        setSelectedId(resp.shards[0].shardId);
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [selectedId]);

  React.useEffect(() => {
    load();
  }, []);

  const selected = shards.find((s) => s.shardId === selectedId) ?? shards[0] ?? null;

  function openSplit(s: ShardInfoProto) {
    setSplitFor(s);
    setSplitKey(s.rangeStart ? s.rangeStart + "m" : "m");
    setSplitStep(0);
    setSplitOpen(true);
  }

  async function runSplit() {
    if (!splitFor) return;
    setSplitting(true);
    setSplitStep(1);

    let stepTimer = 1;
    const iv = setInterval(() => {
      stepTimer += 1;
      setSplitStep(stepTimer);
      if (stepTimer > 9) clearInterval(iv);
    }, 500);

    try {
      const resp = await client.splitShard({ shardId: splitFor.shardId, splitKey });
      clearInterval(iv);
      setSplitStep(10);
      if (resp.ok) {
        toast({
          kind: "ok",
          text: `Shard ${splitFor.shardId} split at "${splitKey}"`,
          sub: `new shard id: ${resp.newShardId}`,
        });
        setTimeout(() => {
          setSplitOpen(false);
          setSplitFor(null);
          setSplitStep(0);
          setSplitting(false);
          load();
        }, 1100);
      } else {
        setSplitting(false);
        setSplitStep(0);
        toast({ kind: "err", text: `Split failed`, sub: resp.error });
      }
    } catch (e) {
      clearInterval(iv);
      setSplitting(false);
      setSplitStep(0);
      toast({ kind: "err", text: "Split error", sub: String(e) });
    }
  }

  if (loading && shards.length === 0) {
    return (
      <div className="fade-in" style={{ padding: 40, color: "var(--fg-3)" }}>
        <span className="spinner" /> Loading shards…
      </div>
    );
  }

  if (error) {
    return (
      <div className="fade-in" style={{ padding: 40 }}>
        <div style={{ color: "var(--err)", marginBottom: 12 }}>Failed to load shards: {error}</div>
        <button className="btn btn-sm" onClick={load}>
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="fade-in">
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="glyph">{RUNES.shards}</span>
            Shards
          </h1>
          <p className="page-sub">
            {shards.length} shard{shards.length !== 1 ? "s" : ""} · multi-raft sharded keyspace
          </p>
        </div>
        <div className="page-actions">
          <button className="btn btn-ghost btn-sm" onClick={load}>
            Refresh
          </button>
          {selected && (
            <button className="btn btn-primary btn-sm" onClick={() => openSplit(selected)}>
              Split shard…
            </button>
          )}
        </div>
      </div>

      {/* Keyspace strip */}
      <div className="panel">
        <div className="panel-head">
          <div className="panel-title">Keyspace map · {shards.length} ranges</div>
          <span className="muted mono" style={{ fontSize: 11 }}>
            equal width (key counts unavailable)
          </span>
        </div>
        <div className="panel-body">
          <div className="shard-strip">
            {shards.map((s) => (
              <div
                key={String(s.shardId)}
                className={`shard-seg ${s.state === "splitting" ? "subtle-stripes" : ""}`}
                style={{
                  flex: 1,
                  background: colorFor(s),
                  outline: selectedId === s.shardId ? "2px solid var(--spark)" : "none",
                  outlineOffset: -2,
                }}
                onClick={() => setSelectedId(s.shardId)}
                title={`shard ${s.shardId} · "${s.rangeStart || "—∞"}" → "${s.rangeEnd || "+∞"}"`}
              >
                <div className="seg-label">{String(s.shardId)}</div>
              </div>
            ))}
          </div>
          <div className="spread" style={{ marginTop: 10, fontFamily: "var(--font-mono)", fontSize: 10.5, color: "var(--fg-4)" }}>
            <span>—∞</span>
            <span>lexicographic UTF-8</span>
            <span>+∞</span>
          </div>
        </div>
      </div>

      <div style={{ height: 18 }} />

      <div className="grid-3">
        {/* Shard table */}
        <div className="panel">
          <div className="panel-head">
            <div className="panel-title">Shards</div>
            <span className="muted mono" style={{ fontSize: 11 }}>
              click to select
            </span>
          </div>
          <div style={{ maxHeight: 460, overflowY: "auto" }}>
            <table className="tbl">
              <thead>
                <tr>
                  <th>id</th>
                  <th>range</th>
                  <th>state</th>
                  <th>leader</th>
                  <th>term</th>
                  <th>applied</th>
                  <th>freshness</th>
                  <th className="num">keys</th>
                  <th className="num">qps</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {shards.map((s) => (
                  <tr
                    key={String(s.shardId)}
                    className={selectedId === s.shardId ? "selected" : ""}
                    onClick={() => setSelectedId(s.shardId)}
                    style={{ cursor: "pointer" }}
                  >
                    <td className="mono">
                      <strong>{String(s.shardId)}</strong>
                    </td>
                    <td className="mono" style={{ color: "var(--fg-3)", fontSize: 11 }}>
                      "{s.rangeStart || "—∞"}" → "{s.rangeEnd || "+∞"}"
                    </td>
                    <td>
                      {s.state === "healthy" && (
                        <span className="pill ok">
                          <span className="pdot" />
                          healthy
                        </span>
                      )}
                      {s.state === "splitting" && (
                        <span className="pill spark">
                          <span className="pdot" />
                          splitting
                        </span>
                      )}
                      {s.state === "under-replicated" && (
                        <span className="pill warn">
                          <span className="pdot" />
                          under-rep
                        </span>
                      )}
                      {!["healthy", "splitting", "under-replicated"].includes(s.state) && (
                        <span className="pill">{s.state || "unknown"}</span>
                      )}
                    </td>
                    <td className="mono">
                      {s.lastObservedAgeMs === undefined ? (
                        <span className="dim">—</span>
                      ) : s.leaderId !== undefined ? (
                        `node-${s.leaderId}`
                      ) : (
                        <span className="dim">none</span>
                      )}
                    </td>
                    <td className="mono">
                      {s.lastObservedAgeMs === undefined ? <span className="dim">—</span> : String(s.term)}
                    </td>
                    <td className="mono">
                      {s.lastObservedAgeMs === undefined ? <span className="dim">—</span> : fmtNum(Number(s.lastApplied))}
                    </td>
                    <td>
                      <FreshnessBadge ageMs={s.lastObservedAgeMs} />
                    </td>
                    <td className="num dim" title="unavailable — no key-count API yet">
                      —
                    </td>
                    <td className="num dim" title="unavailable — requires metrics backend">
                      —
                    </td>
                    <td>
                      <div className="row-actions">
                        <button
                          className="btn btn-ghost btn-sm"
                          onClick={(e) => {
                            e.stopPropagation();
                            openSplit(s);
                          }}
                        >
                          split
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Selected shard detail */}
        {selected && (
          <div className="panel">
            <div className="panel-head">
              <div className="panel-title">shard {String(selected.shardId)}</div>
              <FreshnessBadge ageMs={selected.lastObservedAgeMs} />
            </div>
            <div className="panel-body">
              <dl className="kvp">
                <dt>shard_id</dt>
                <dd>{String(selected.shardId)}</dd>
                <dt>range_start</dt>
                <dd>"{selected.rangeStart || "—∞"}"</dd>
                <dt>range_end</dt>
                <dd>"{selected.rangeEnd || "+∞"}"</dd>
                <dt>state</dt>
                <dd>{selected.state}</dd>
                <dt>leader</dt>
                <dd>
                  {selected.lastObservedAgeMs === undefined ? (
                    <span className="dim">— (no snapshot)</span>
                  ) : selected.leaderId !== undefined ? (
                    `node-${selected.leaderId}`
                  ) : (
                    <span className="dim">none</span>
                  )}
                </dd>
                <dt>voters</dt>
                <dd>{selected.voters.map((v) => `node-${v}`).join(", ") || "—"}</dd>
                {selected.learners.length > 0 && (
                  <>
                    <dt>learners</dt>
                    <dd>{selected.learners.map((l) => `node-${l}`).join(", ")}</dd>
                  </>
                )}
                <dt>term</dt>
                <dd>
                  {selected.lastObservedAgeMs === undefined ? (
                    <span className="dim">—</span>
                  ) : (
                    String(selected.term)
                  )}
                </dd>
                <dt>last_applied</dt>
                <dd>
                  {selected.lastObservedAgeMs === undefined ? (
                    <span className="dim">—</span>
                  ) : (
                    fmtNum(Number(selected.lastApplied))
                  )}
                </dd>
                <dt>keys</dt>
                <dd>
                  <span className="dim" title="unavailable">
                    —
                  </span>
                </dd>
                <dt>size</dt>
                <dd>
                  <span className="dim" title="unavailable">
                    —
                  </span>
                </dd>
                <dt>qps</dt>
                <dd>
                  <span className="dim" title="unavailable">
                    —
                  </span>
                </dd>
                <dt>p99</dt>
                <dd>
                  <span className="dim" title="unavailable">
                    —
                  </span>
                </dd>
              </dl>
              <div className="divider" />
              <div className="hstack" style={{ flexWrap: "wrap" }}>
                <button className="btn btn-sm btn-primary" onClick={() => openSplit(selected)}>
                  Split…
                </button>
                <button className="btn btn-sm" disabled title="not yet implemented">
                  Compact
                </button>
                <button className="btn btn-sm" disabled title="not yet implemented">
                  Snapshot
                </button>
                <button className="btn btn-sm btn-danger" disabled title="not yet implemented">
                  Drain
                </button>
              </div>
              <div className="divider" />
              <div style={{ fontSize: 11, color: "var(--fg-3)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 6 }}>
                storage key prefix
              </div>
              <div
                className="mono"
                style={{ fontSize: 11.5, color: "var(--fg-2)", background: "var(--bg-sunken)", padding: "8px 10px", borderRadius: 6, border: "1px solid var(--line-soft)" }}
              >
                be_u64({String(selected.shardId)}) · {be_u64_hex(selected.shardId)}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Split modal */}
      {splitOpen && splitFor && (
        <div
          className="modal-veil"
          onClick={(e) => {
            if (e.target === e.currentTarget && !splitting) setSplitOpen(false);
          }}
        >
          <div className="modal" style={{ width: 560 }}>
            <div className="modal-head">
              <div className="modal-title">
                <span className="rune" style={{ color: "var(--spark)", marginRight: 8 }}>
                  {RUNES.shards}
                </span>
                Split shard {String(splitFor.shardId)}
              </div>
              {!splitting && (
                <button className="btn btn-ghost btn-sm" onClick={() => setSplitOpen(false)}>
                  Cancel
                </button>
              )}
            </div>
            <div className="modal-body">
              {splitStep === 0 ? (
                <>
                  <p className="muted" style={{ marginTop: 0, fontSize: 12.5 }}>
                    Splits the shard into two ranges at <span className="mono">split_key</span>. Writes to the source shard are{" "}
                    <strong>blocked</strong> for the duration of the split.
                  </p>
                  <div className="field">
                    <label>shard_id</label>
                    <input value={String(splitFor.shardId)} disabled />
                  </div>
                  <div className="field">
                    <label>current range</label>
                    <input value={`"${splitFor.rangeStart || "—∞"}" → "${splitFor.rangeEnd || "+∞"}"`} disabled />
                  </div>
                  <div className="field">
                    <label>split_key</label>
                    <input value={splitKey} onChange={(e) => setSplitKey(e.target.value)} placeholder="must be inside the shard's range" />
                    <span className="hint">All keys ≥ split_key move to the new shard.</span>
                  </div>
                </>
              ) : (
                <div>
                  <div style={{ fontSize: 12.5, marginBottom: 12, color: "var(--fg-2)" }}>
                    SplitCoordinator → shard {String(splitFor.shardId)} at "{splitKey}"
                  </div>
                  {SPLIT_STEPS.map((label, i) => {
                    const stepNum = i + 1;
                    const done = splitStep > stepNum;
                    const active = splitStep === stepNum;
                    return (
                      <div
                        key={i}
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: 10,
                          padding: "6px 0",
                          opacity: splitStep < stepNum ? 0.4 : 1,
                          transition: "opacity .2s",
                        }}
                      >
                        <span style={{ width: 18, display: "inline-flex", justifyContent: "center" }}>
                          {done ? (
                            <span style={{ color: "var(--ok)", fontFamily: "var(--font-mono)" }}>✓</span>
                          ) : active ? (
                            <span className="spinner" />
                          ) : (
                            <span className="mono dim">{stepNum}</span>
                          )}
                        </span>
                        <span className="mono" style={{ fontSize: 12, color: done ? "var(--fg-3)" : active ? "var(--fg)" : "var(--fg-3)" }}>
                          {stepNum}. {label}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
            <div className="modal-foot">
              {splitStep === 0 ? (
                <>
                  <button className="btn btn-ghost btn-sm" onClick={() => setSplitOpen(false)}>
                    Cancel
                  </button>
                  <button className="btn btn-primary btn-sm" onClick={runSplit} disabled={!splitKey}>
                    SplitShard →
                  </button>
                </>
              ) : (
                <span className="muted mono" style={{ fontSize: 11 }}>
                  {splitStep > 9 ? "complete · resuming writes" : `step ${splitStep}/9 · writes blocked on shard ${splitFor.shardId}`}
                </span>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
