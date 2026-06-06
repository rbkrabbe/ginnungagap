import React from "react";
import { createPromiseClient } from "@connectrpc/connect";
import { KvService } from "../gen/ginnungagap/v1/kv_connect";
import { AdminService } from "../gen/ginnungagap/v1/cluster_connect";
import { clientTransport, clusterTransport } from "../transport";
import { KeyValue } from "../gen/ginnungagap/v1/types_pb";
import type { ShardInfoProto } from "../gen/ginnungagap/v1/cluster_pb";

const kvClient = createPromiseClient(KvService, clientTransport);
const adminClient = createPromiseClient(AdminService, clusterTransport);

function fmtBytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function fmtNum(n: number) {
  return n.toLocaleString();
}

function bytesToHex(arr: Uint8Array): string {
  return Array.from(arr).map((b) => b.toString(16).padStart(2, "0")).join(" ");
}

function bytesToAscii(arr: Uint8Array): string {
  return Array.from(arr).map((b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : ".")).join("");
}

function detectType(val: Uint8Array): "json" | "string" | "bytes" {
  try {
    const str = new TextDecoder().decode(val).trim();
    JSON.parse(str);
    if (str.startsWith("{") || str.startsWith("[")) return "json";
    return "string";
  } catch {
    return "bytes";
  }
}

function valueAsString(val: Uint8Array): string {
  try {
    return new TextDecoder().decode(val);
  } catch {
    return "<binary>";
  }
}

// Build prefix tree from key list
interface TreeNode {
  name: string;
  path: string;
  children: Record<string, TreeNode>;
  count: number;
}

function buildPrefixTree(keys: string[]): TreeNode {
  const root: TreeNode = { name: "/", path: "/", children: {}, count: 0 };
  for (const key of keys) {
    const parts = key.split("/").filter(Boolean);
    let node = root;
    node.count++;
    let acc = "";
    for (let i = 0; i < parts.length - 1; i++) {
      acc += "/" + parts[i];
      if (!node.children[parts[i]]) {
        node.children[parts[i]] = { name: parts[i], path: acc, children: {}, count: 0 };
      }
      node = node.children[parts[i]];
      node.count++;
    }
  }
  return root;
}

function shardForKey(key: string, shards: ShardInfoProto[]): bigint | null {
  const norm = key.replace(/^\//, "").toLowerCase();
  for (const s of shards) {
    const start = s.rangeStart || "";
    const end = s.rangeEnd || "￿￿";
    if (norm >= start && norm < end) return s.shardId;
  }
  return shards[0]?.shardId ?? null;
}

function TreeNodeCmp({
  node,
  depth,
  prefix,
  setPrefix,
  openMap,
  setOpenMap,
}: {
  node: TreeNode;
  depth: number;
  prefix: string;
  setPrefix: (p: string) => void;
  openMap: Record<string, boolean>;
  setOpenMap: React.Dispatch<React.SetStateAction<Record<string, boolean>>>;
}) {
  const kids = Object.values(node.children).sort((a, b) => a.name.localeCompare(b.name));
  const hasKids = kids.length > 0;
  const isOpen = !!openMap[node.path] || depth === 0;
  const isActive = prefix === node.path;
  return (
    <>
      <div className={`kv-tree-row ${isActive ? "active" : ""}`} style={{ paddingLeft: 8 + depth * 14 }} onClick={() => setPrefix(node.path)}>
        {hasKids ? (
          <span className="kv-twist" onClick={(e) => { e.stopPropagation(); setOpenMap((m) => ({ ...m, [node.path]: !isOpen })); }}>
            {isOpen ? "▾" : "▸"}
          </span>
        ) : (
          <span className="kv-twist" style={{ visibility: "hidden" }}>·</span>
        )}
        <span className="kv-tree-name">{depth === 0 ? "/" : node.name}</span>
        <span className="kv-tree-count">{node.count}</span>
      </div>
      {isOpen && hasKids && kids.map((k) => (
        <TreeNodeCmp key={k.path} node={k} depth={depth + 1} prefix={prefix} setPrefix={setPrefix} openMap={openMap} setOpenMap={setOpenMap} />
      ))}
    </>
  );
}

interface KvRow {
  key: string;
  kv: KeyValue;
  type: "json" | "string" | "bytes";
  shardId: bigint | null;
}

export default function KvScreen({ toast }: { toast: (t: { kind?: string; text: string; sub?: string }) => void }) {
  const [rows, setRows] = React.useState<KvRow[]>([]);
  const [shards, setShards] = React.useState<ShardInfoProto[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  const [prefix, setPrefix] = React.useState("/");
  const [query, setQuery] = React.useState("");
  const [shardFilter, setShardFilter] = React.useState("all");
  const [selectedKey, setSelectedKey] = React.useState<string | null>(null);
  const [openMap, setOpenMap] = React.useState<Record<string, boolean>>({ "/": true });

  const [editing, setEditing] = React.useState(false);
  const [editText, setEditText] = React.useState("");
  const [putOpen, setPutOpen] = React.useState(false);
  const [delOpen, setDelOpen] = React.useState(false);
  const [casOpen, setCasOpen] = React.useState(false);
  const [inspectTab, setInspectTab] = React.useState<"value" | "meta">("value");

  const [opLoading, setOpLoading] = React.useState(false);

  const loadShards = React.useCallback(async () => {
    try {
      const resp = await adminClient.listShards({});
      setShards(resp.shards);
    } catch (_e) {
      // shards optional for key routing display
    }
  }, []);

  const scan = React.useCallback(async (startKey: string) => {
    setLoading(true);
    setError(null);
    try {
      const resp = await kvClient.scan({ startKey, endKey: "", limit: 500 });
      const newRows: KvRow[] = resp.kvs.map((kv) => ({
        key: kv.key,
        kv,
        type: detectType(kv.value),
        shardId: shardForKey(kv.key, shards),
      }));
      setRows(newRows);
      if (newRows.length > 0 && (selectedKey === null || !newRows.find((r) => r.key === selectedKey))) {
        setSelectedKey(newRows[0].key);
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [shards, selectedKey]);

  React.useEffect(() => {
    loadShards().then(() => scan(""));
  }, []);

  const tree = React.useMemo(() => buildPrefixTree(rows.map((r) => r.key)), [rows]);

  const filtered = React.useMemo(() => {
    return rows.filter((r) => {
      if (prefix !== "/" && !r.key.startsWith(prefix + "/") && r.key !== prefix) return false;
      if (shardFilter !== "all" && String(r.shardId) !== shardFilter) return false;
      if (query && !r.key.toLowerCase().includes(query.toLowerCase())) return false;
      return true;
    });
  }, [rows, prefix, shardFilter, query]);

  React.useEffect(() => {
    if (!selectedKey || !filtered.find((r) => r.key === selectedKey)) {
      setSelectedKey(filtered[0]?.key ?? null);
    }
  }, [filtered]);

  const selectedRow = rows.find((r) => r.key === selectedKey) ?? null;

  async function doGet(key: string) {
    setOpLoading(true);
    try {
      const resp = await kvClient.get({ key });
      if (resp.kv) {
        const type = detectType(resp.kv.value);
        setRows((prev) => {
          const idx = prev.findIndex((r) => r.key === key);
          if (idx >= 0) {
            const next = [...prev];
            next[idx] = { key, kv: resp.kv!, type, shardId: shardForKey(key, shards) };
            return next;
          }
          return [...prev, { key, kv: resp.kv!, type, shardId: shardForKey(key, shards) }];
        });
      }
    } catch (e) {
      toast({ kind: "err", text: `Get failed`, sub: String(e) });
    } finally {
      setOpLoading(false);
    }
  }

  async function doPut(key: string, value: string) {
    setOpLoading(true);
    try {
      const enc = new TextEncoder().encode(value);
      const resp = await kvClient.put({ key, value: enc });
      const type = detectType(enc);
      const newKv = new KeyValue({ key, value: enc, version: resp.newVersion, createdAtNs: 0n, modifiedAtNs: 0n, expiresAtNs: 0n });
      setRows((prev) => {
        const idx = prev.findIndex((r) => r.key === key);
        if (idx >= 0) {
          const next = [...prev];
          next[idx] = { key, kv: newKv, type, shardId: shardForKey(key, shards) };
          return next;
        }
        const newRow: KvRow = { key, kv: newKv, type, shardId: shardForKey(key, shards) };
        return [...prev, newRow].sort((a, b) => (a.key < b.key ? -1 : 1));
      });
      setSelectedKey(key);
      toast({ kind: "ok", text: `Put ${key}`, sub: `KvService/Put · ${enc.length} B` });
    } catch (e) {
      toast({ kind: "err", text: "Put failed", sub: String(e) });
    } finally {
      setOpLoading(false);
    }
  }

  async function doDelete(key: string) {
    setOpLoading(true);
    try {
      await kvClient.delete({ key });
      setRows((prev) => prev.filter((r) => r.key !== key));
      setSelectedKey(null);
      toast({ kind: "err", text: `Deleted ${key}`, sub: "KvService/Delete" });
    } catch (e) {
      toast({ kind: "err", text: "Delete failed", sub: String(e) });
    } finally {
      setOpLoading(false);
      setDelOpen(false);
    }
  }

  async function doCas(key: string, expectedValue: Uint8Array, newValue: string) {
    setOpLoading(true);
    try {
      const enc = new TextEncoder().encode(newValue);
      const resp = await kvClient.compareAndSwap({ key, expectedValue: expectedValue as Uint8Array<ArrayBuffer>, newValue: enc });
      if (resp.success) {
        const type = detectType(enc);
        const newKv = resp.current ?? new KeyValue({ key, value: enc, version: 0n, createdAtNs: 0n, modifiedAtNs: 0n, expiresAtNs: 0n });
        setRows((prev) => prev.map((r) => (r.key === key ? { key, kv: newKv, type, shardId: r.shardId } : r)));
        toast({ kind: "ok", text: `CAS ok · ${key}`, sub: "KvService/CompareAndSwap · committed" });
        setEditing(false);
        setCasOpen(false);
      } else {
        toast({ kind: "err", text: "CAS conflict", sub: "ABORTED — expected value mismatch" });
      }
    } catch (e) {
      toast({ kind: "err", text: "CAS error", sub: String(e) });
    } finally {
      setOpLoading(false);
    }
  }

  function renderValueDisplay(row: KvRow) {
    if (row.type === "bytes") {
      const arr = row.kv.value;
      const lines: { off: string; hex: string; ascii: string }[] = [];
      for (let i = 0; i < arr.length; i += 16) {
        const chunk = arr.slice(i, i + 16);
        lines.push({ off: i.toString(16).padStart(8, "0"), hex: bytesToHex(chunk).padEnd(48, " "), ascii: bytesToAscii(chunk) });
      }
      return (
        <pre className="kv-hexdump">
          {lines.map((l, i) => (
            <div key={i}>
              <span className="kv-hex-off">{l.off}</span>
              <span className="kv-hex-hex">{"  " + l.hex + "  "}</span>
              <span className="kv-hex-ascii">{l.ascii}</span>
            </div>
          ))}
        </pre>
      );
    }
    return <pre className="kv-value">{valueAsString(row.kv.value)}</pre>;
  }

  const totalSize = filtered.reduce((a, r) => a + r.kv.value.length, 0);

  return (
    <div className="fade-in">
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="glyph">ᚲ</span>
            KV browser
          </h1>
          <p className="page-sub">
            <span className="mono">{prefix}</span> · {filtered.length} key{filtered.length !== 1 ? "s" : ""} · {fmtBytes(totalSize)}
            {prefix !== "/" && (
              <button className="btn btn-ghost btn-sm" style={{ marginLeft: 10, padding: "1px 6px", fontSize: 11 }} onClick={() => setPrefix("/")}>
                clear
              </button>
            )}
          </p>
        </div>
        <div className="page-actions">
          <button className="btn btn-ghost btn-sm" onClick={() => scan("")}>Refresh</button>
          <button className="btn btn-sm" onClick={() => { setEditText('{\n  "example": true\n}'); setPutOpen(true); }}>+ Put key</button>
        </div>
      </div>

      {error && (
        <div style={{ marginBottom: 14, padding: "10px 14px", background: "var(--err-bg)", border: "1px solid var(--err)", borderRadius: "var(--radius)", color: "var(--err)", fontSize: 12.5 }}>
          {error}
          <button className="btn btn-ghost btn-sm" style={{ marginLeft: 12 }} onClick={() => scan("")}>Retry</button>
        </div>
      )}

      {/* Op bar */}
      <div className="kv-opbar">
        <div className="kv-search">
          <span className="muted mono">⌕</span>
          <input placeholder="search keys…" value={query} onChange={(e) => setQuery(e.target.value)} />
          {query && <button className="btn btn-ghost btn-sm" onClick={() => setQuery("")} style={{ padding: "1px 6px" }}>×</button>}
        </div>
        <div className="kv-filter">
          <label className="mono">shard</label>
          <select value={shardFilter} onChange={(e) => setShardFilter(e.target.value)}>
            <option value="all">any</option>
            {shards.map((s) => (
              <option key={String(s.shardId)} value={String(s.shardId)}>
                shard {String(s.shardId)} · "{s.rangeStart || "—∞"}" → "{s.rangeEnd || "+∞"}"
              </option>
            ))}
          </select>
        </div>
        <div style={{ flex: 1 }} />
        <div className="kv-rpc-hint mono">
          <span className="muted">→</span>
          <span>KvService/Scan</span>
          <span className="muted">prefix=</span>
          <span style={{ color: "var(--spark)" }}>"{prefix}"</span>
          <span className="muted">limit=500</span>
        </div>
      </div>

      {loading && rows.length === 0 ? (
        <div style={{ padding: 40, color: "var(--fg-3)" }}><span className="spinner" /> Loading keys…</div>
      ) : (
        <div className="kv-grid">
          {/* Left: prefix tree */}
          <div className="panel kv-tree-panel">
            <div className="panel-head">
              <div className="panel-title">Prefix tree</div>
              <span className="muted mono" style={{ fontSize: 11 }}>{rows.length} keys</span>
            </div>
            <div className="kv-tree">
              <TreeNodeCmp node={tree} depth={0} prefix={prefix} setPrefix={setPrefix} openMap={openMap} setOpenMap={setOpenMap} />
            </div>
          </div>

          {/* Middle: key list */}
          <div className="panel kv-list-panel">
            <div className="panel-head">
              <div className="panel-title">Keys</div>
              <span className="muted mono" style={{ fontSize: 11 }}>
                {filtered.length === 0 ? "no matches" : `${filtered.length} · ${fmtBytes(totalSize)}`}
              </span>
            </div>
            <div className="kv-list-scroll">
              {filtered.length === 0 ? (
                <div className="kv-empty">
                  <div className="rune" style={{ fontSize: 28, color: "var(--fg-4)" }}>ᚲ</div>
                  <div className="muted" style={{ marginTop: 8, fontSize: 12.5 }}>No keys match this prefix + filter.</div>
                  <button className="btn btn-sm" style={{ marginTop: 12 }} onClick={() => { setQuery(""); setShardFilter("all"); setPrefix("/"); }}>reset</button>
                </div>
              ) : (
                <table className="tbl kv-list-tbl">
                  <thead>
                    <tr>
                      <th>key</th>
                      <th style={{ width: 70 }}>shard</th>
                      <th style={{ width: 60 }}>type</th>
                      <th className="num" style={{ width: 70 }}>size</th>
                      <th className="num" style={{ width: 100 }}>version</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((r) => (
                      <tr
                        key={r.key}
                        className={selectedKey === r.key ? "selected" : ""}
                        onClick={() => { setSelectedKey(r.key); setEditing(false); }}
                        style={{ cursor: "pointer" }}
                      >
                        <td className="mono kv-key-cell" title={r.key}>{r.key}</td>
                        <td className="mono dim">{r.shardId !== null ? String(r.shardId) : "?"}</td>
                        <td>
                          <span className={`pill ${r.type === "json" ? "frost" : r.type === "bytes" ? "spark" : ""}`} style={{ fontSize: 10, padding: "1px 6px" }}>
                            {r.type}
                          </span>
                        </td>
                        <td className="num">{fmtBytes(r.kv.value.length)}</td>
                        <td className="num dim">{fmtNum(Number(r.kv.version))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          {/* Right: inspector */}
          <div className="panel kv-inspect-panel">
            {!selectedRow ? (
              <div className="kv-empty" style={{ padding: 40 }}>
                <div className="rune" style={{ fontSize: 28, color: "var(--fg-4)" }}>ᚱ</div>
                <div className="muted" style={{ marginTop: 8, fontSize: 12.5 }}>Select a key to inspect.</div>
              </div>
            ) : (
              <>
                <div className="panel-head" style={{ display: "block" }}>
                  <div className="spread">
                    <div className="panel-title">Inspect</div>
                    <span className={`pill ${selectedRow.type === "json" ? "frost" : selectedRow.type === "bytes" ? "spark" : ""}`} style={{ fontSize: 10 }}>
                      {selectedRow.type}
                    </span>
                  </div>
                  <div className="kv-inspect-key mono" title={selectedRow.key}>{selectedRow.key}</div>
                </div>

                <div className="panel-body" style={{ paddingBottom: 8 }}>
                  <dl className="kvp" style={{ gridTemplateColumns: "100px 1fr" }}>
                    <dt>shard</dt>
                    <dd>{selectedRow.shardId !== null ? `shard ${selectedRow.shardId}` : "—"}</dd>
                    <dt>size</dt>
                    <dd>{fmtBytes(selectedRow.kv.value.length)} ({selectedRow.kv.value.length.toLocaleString()} B)</dd>
                    <dt>version</dt>
                    <dd>{fmtNum(Number(selectedRow.kv.version))}</dd>
                    <dt>expires</dt>
                    <dd>
                      {selectedRow.kv.expiresAtNs && selectedRow.kv.expiresAtNs > 0n
                        ? new Date(Number(selectedRow.kv.expiresAtNs / 1_000_000n)).toISOString()
                        : <span className="dim" title="no TTL">—</span>}
                    </dd>
                  </dl>
                </div>

                <div className="kv-inspect-tabs">
                  <div className={`kv-tab ${inspectTab === "value" ? "active" : ""}`} onClick={() => setInspectTab("value")}>value</div>
                  <div className={`kv-tab ${inspectTab === "meta" ? "active" : ""}`} onClick={() => setInspectTab("meta")}>meta</div>
                  <div className="kv-tab" style={{ opacity: 0.4, cursor: "not-allowed" }} title="Watch not supported over gRPC-web (bidirectional streaming)">
                    watch
                  </div>
                  <span style={{ flex: 1 }} />
                  {!editing && selectedRow.type !== "bytes" && inspectTab === "value" && (
                    <button className="btn btn-ghost btn-sm" onClick={() => { setEditing(true); setEditText(valueAsString(selectedRow.kv.value)); }}>Edit</button>
                  )}
                </div>

                <div className="kv-value-wrap">
                  {inspectTab === "value" ? (
                    editing ? (
                      <textarea className="kv-value-edit mono" value={editText} onChange={(e) => setEditText(e.target.value)} spellCheck={false} />
                    ) : (
                      renderValueDisplay(selectedRow)
                    )
                  ) : (
                    <pre className="kv-value" style={{ fontSize: 11 }}>
                      {JSON.stringify({
                        key: selectedRow.kv.key,
                        version: String(selectedRow.kv.version),
                        created_at_ns: String(selectedRow.kv.createdAtNs),
                        modified_at_ns: String(selectedRow.kv.modifiedAtNs),
                        expires_at_ns: String(selectedRow.kv.expiresAtNs),
                      }, null, 2)}
                    </pre>
                  )}
                </div>

                <div className="panel-foot kv-inspect-foot">
                  {editing ? (
                    <>
                      <span className="muted mono" style={{ fontSize: 11 }}>{new Blob([editText]).size} B</span>
                      <span style={{ flex: 1 }} />
                      <button className="btn btn-ghost btn-sm" onClick={() => setEditing(false)}>Cancel</button>
                      <button className="btn btn-sm" onClick={() => setCasOpen(true)}>CAS…</button>
                      <button className="btn btn-sm btn-primary" disabled={opLoading} onClick={() => {
                        doPut(selectedRow.key, editText).then(() => setEditing(false));
                      }}>
                        {opLoading ? <span className="spinner" /> : "Put →"}
                      </button>
                    </>
                  ) : (
                    <>
                      <button className="btn btn-ghost btn-sm" onClick={() => doGet(selectedRow.key)}>
                        {opLoading ? <span className="spinner" /> : "Refresh"}
                      </button>
                      <button
                        className="btn btn-ghost btn-sm"
                        style={{ opacity: 0.5, cursor: "not-allowed" }}
                        title="Watch is not supported over gRPC-web (requires bidirectional streaming)"
                        onClick={() => toast({ kind: "err", text: "Watch unavailable", sub: "gRPC-web does not support bidirectional streaming" })}
                      >
                        Watch
                      </button>
                      <span style={{ flex: 1 }} />
                      <button className="btn btn-sm btn-danger" onClick={() => setDelOpen(true)}>Delete</button>
                    </>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Put modal */}
      {putOpen && (
        <PutModal
          shards={shards}
          initialValue={editText}
          onClose={() => setPutOpen(false)}
          onPut={(k, v) => { doPut(k, v); setPutOpen(false); setSelectedKey(k); }}
        />
      )}

      {/* Delete confirm */}
      {delOpen && selectedRow && (
        <div className="modal-veil" onClick={(e) => { if (e.target === e.currentTarget) setDelOpen(false); }}>
          <div className="modal" style={{ width: 460 }}>
            <div className="modal-head">
              <div className="modal-title"><span className="rune" style={{ color: "var(--err)", marginRight: 8 }}>ᚺ</span>Delete key</div>
            </div>
            <div className="modal-body">
              <p className="muted" style={{ marginTop: 0, fontSize: 12.5 }}>
                Issues <span className="mono">KvService/Delete</span> for the key below. The tombstone is replicated through the Raft group before the call returns.
              </p>
              <div className="field"><label>key</label><input value={selectedRow.key} disabled /></div>
              <div className="field"><label>version</label><input value={fmtNum(Number(selectedRow.kv.version))} disabled /></div>
            </div>
            <div className="modal-foot">
              <button className="btn btn-ghost btn-sm" onClick={() => setDelOpen(false)}>Cancel</button>
              <button className="btn btn-sm btn-danger" disabled={opLoading} onClick={() => doDelete(selectedRow.key)}>
                {opLoading ? <span className="spinner" /> : "Delete →"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* CAS modal */}
      {casOpen && selectedRow && (
        <div className="modal-veil" onClick={(e) => { if (e.target === e.currentTarget) setCasOpen(false); }}>
          <div className="modal" style={{ width: 520 }}>
            <div className="modal-head">
              <div className="modal-title"><span className="rune" style={{ color: "var(--spark)", marginRight: 8 }}>ᛇ</span>CompareAndSwap</div>
            </div>
            <div className="modal-body">
              <p className="muted" style={{ marginTop: 0, fontSize: 12.5 }}>
                <span className="mono">KvService/CompareAndSwap</span> — commits only if the on-disk value matches the expected bytes. Mismatch → <span className="mono">ABORTED</span>.
              </p>
              <div className="field"><label>key</label><input value={selectedRow.key} disabled /></div>
              <div className="field">
                <label>expected value (current)</label>
                <input value={valueAsString(selectedRow.kv.value).slice(0, 80)} disabled />
              </div>
              <div className="field">
                <label>new value · {new Blob([editText]).size} B</label>
                <textarea rows={6} value={editText} onChange={(e) => setEditText(e.target.value)} />
              </div>
            </div>
            <div className="modal-foot">
              <button className="btn btn-ghost btn-sm" onClick={() => setCasOpen(false)}>Cancel</button>
              <button className="btn btn-sm btn-primary" disabled={opLoading} onClick={() => doCas(selectedRow.key, selectedRow.kv.value, editText)}>
                {opLoading ? <span className="spinner" /> : "CompareAndSwap →"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function PutModal({ shards, initialValue, onClose, onPut }: {
  shards: ShardInfoProto[];
  initialValue: string;
  onClose: () => void;
  onPut: (key: string, value: string) => void;
}) {
  const [key, setKey] = React.useState("/");
  const [value, setValue] = React.useState(initialValue || '{\n  "example": true\n}');

  const norm = key.replace(/^\//, "").toLowerCase();
  const shardPreview = shards.find((s) => {
    const start = s.rangeStart || "";
    const end = s.rangeEnd || "￿￿";
    return norm >= start && norm < end;
  }) ?? null;

  const valid = key && key.startsWith("/") && key.length > 1 && value.length > 0;

  return (
    <div className="modal-veil" onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal" style={{ width: 580 }}>
        <div className="modal-head">
          <div className="modal-title"><span className="rune" style={{ color: "var(--spark)", marginRight: 8 }}>ᚲ</span>Put key</div>
        </div>
        <div className="modal-body">
          <div className="field">
            <label>key</label>
            <input value={key} onChange={(e) => setKey(e.target.value)} placeholder="/namespace/key" autoFocus />
            <span className="hint">
              {shardPreview
                ? <>routes to <span style={{ color: "var(--spark)" }}>shard {String(shardPreview.shardId)}</span> · leader {shardPreview.leaderId !== undefined ? `node-${shardPreview.leaderId}` : "unknown"}</>
                : "—"}
            </span>
          </div>
          <div className="field">
            <label>value · {new Blob([value]).size} B</label>
            <textarea rows={8} value={value} onChange={(e) => setValue(e.target.value)} spellCheck={false} />
          </div>
        </div>
        <div className="modal-foot">
          <span className="muted mono" style={{ fontSize: 11 }}>KvService/Put</span>
          <span style={{ flex: 1 }} />
          <button className="btn btn-ghost btn-sm" onClick={onClose}>Cancel</button>
          <button className="btn btn-sm btn-primary" disabled={!valid} onClick={() => onPut(key, value)}>Put →</button>
        </div>
      </div>
    </div>
  );
}
