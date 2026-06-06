import React from "react";
import { createPromiseClient } from "@connectrpc/connect";
import { AdminService } from "./gen/ginnungagap/v1/cluster_connect";
import { clusterTransport } from "./transport";
import type { ShardInfoProto } from "./gen/ginnungagap/v1/cluster_pb";
import { useTweaks, TweaksPanel, TweakSection, TweakRadio, TweakSlider, TweakToggle } from "./tweaks-panel";

import OverviewScreen from "./screens/Overview";
import NodesScreen from "./screens/Nodes";
import ShardsScreen from "./screens/Shards";
import MembershipScreen from "./screens/Membership";
import KvScreen from "./screens/Kv";

const adminClient = createPromiseClient(AdminService, clusterTransport);

const TWEAK_DEFAULTS = {
  theme: "dark",
  density: "balanced",
  accentHue: 65,
  monoEverywhere: false,
  showRunes: true,
};

type NavId = "overview" | "nodes" | "shards" | "membership" | "kv";

const NAV: Array<{ id: NavId; label: string; glyph: string }> = [
  { id: "overview", label: "Overview", glyph: "ᚷ" },
  { id: "nodes", label: "Nodes", glyph: "ᚾ" },
  { id: "shards", label: "Shards", glyph: "ᛊ" },
  { id: "membership", label: "Membership", glyph: "ᛚ" },
];

const NAV_DATA: Array<{ id: NavId; label: string; glyph: string }> = [
  { id: "kv", label: "KV browser", glyph: "ᚲ" },
];

const NAV_DISABLED = [
  { id: "watch", label: "Watch streams", glyph: "ᚷ" },
  { id: "metrics", label: "Metrics explorer", glyph: "ᛗ" },
  { id: "audit", label: "Audit log", glyph: "ᚨ" },
  { id: "settings", label: "Settings", glyph: "ᛇ" },
];

type Toast = { id: string; kind?: string; text: string; sub?: string };

function Toasts({ items }: { items: Toast[] }) {
  return (
    <div className="toast-rail">
      {items.map((t) => (
        <div key={t.id} className={`toast ${t.kind ?? ""}`}>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 500 }}>{t.text}</div>
            {t.sub && <div className="toast-mono">{t.sub}</div>}
          </div>
        </div>
      ))}
    </div>
  );
}

function useClusterPill() {
  const [shards, setShards] = React.useState<ShardInfoProto[]>([]);

  React.useEffect(() => {
    let cancelled = false;
    async function fetch() {
      try {
        const resp = await adminClient.listShards({});
        if (!cancelled) setShards(resp.shards);
      } catch {
        // best-effort; cluster pill shows "—" on error
      }
    }
    fetch();
    const iv = setInterval(fetch, 15_000);
    return () => {
      cancelled = true;
      clearInterval(iv);
    };
  }, []);

  const shardCount = shards.length;

  const nodeIds = React.useMemo(() => {
    const ids = new Set<bigint>();
    for (const s of shards) {
      if (s.leaderId != null) ids.add(s.leaderId);
      for (const v of s.voters) ids.add(v);
      for (const l of s.learners) ids.add(l);
    }
    return ids;
  }, [shards]);

  const term = React.useMemo(() => {
    let best: bigint | undefined;
    for (const s of shards) {
      if (s.lastObservedAgeMs !== undefined) {
        if (best === undefined || s.term > best) best = s.term;
      }
    }
    return best;
  }, [shards]);

  return { shardCount, nodeCount: nodeIds.size, term };
}

export default function App() {
  const [tweaks, setTweak] = useTweaks(TWEAK_DEFAULTS);
  const [screen, setScreen] = React.useState<NavId>("overview");
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const { shardCount, nodeCount, term } = useClusterPill();

  React.useEffect(() => {
    document.documentElement.dataset.theme = tweaks.theme;
    document.documentElement.dataset.density = tweaks.density;
    if (tweaks.accentHue !== 65) {
      document.documentElement.style.setProperty("--spark", `oklch(0.78 0.14 ${tweaks.accentHue})`);
      document.documentElement.style.setProperty("--spark-dim", `oklch(0.58 0.10 ${tweaks.accentHue})`);
      document.documentElement.style.setProperty("--spark-bg", `oklch(0.30 0.06 ${tweaks.accentHue})`);
    } else {
      document.documentElement.style.removeProperty("--spark");
      document.documentElement.style.removeProperty("--spark-dim");
      document.documentElement.style.removeProperty("--spark-bg");
    }
    document.documentElement.style[tweaks.monoEverywhere ? "setProperty" : "removeProperty"](
      "--font-ui",
      "var(--font-mono)"
    );
  }, [tweaks]);

  function pushToast(t: { kind?: string; text: string; sub?: string }) {
    const id = Math.random().toString(36).slice(2);
    setToasts((prev) => [...prev, { ...t, id }]);
    setTimeout(() => setToasts((prev) => prev.filter((x) => x.id !== id)), 3500);
  }

  const crumbName = [...NAV, ...NAV_DATA].find((n) => n.id === screen)?.label ?? "—";

  function renderScreen() {
    switch (screen) {
      case "overview":
        return <OverviewScreen />;
      case "nodes":
        return <NodesScreen toast={pushToast} />;
      case "shards":
        return <ShardsScreen toast={pushToast} />;
      case "membership":
        return <MembershipScreen toast={pushToast} />;
      case "kv":
        return <KvScreen toast={pushToast} />;
    }
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" />
          <div>
            <div className="brand-name">Ginnungagap</div>
            <div className="brand-sub">Console</div>
          </div>
        </div>

        <div className="cluster-pill">
          <div className="cl-row">
            <span className="cl-name">local</span>
            <span className="cl-meta">▾</span>
          </div>
          <div className="cl-meta">
            <span className="pill ok" style={{ fontSize: 9.5, padding: "1px 5px" }}>
              <span className="pdot" />
            </span>
            {nodeCount > 0 ? `${nodeCount} node${nodeCount !== 1 ? "s" : ""}` : "—"} ·{" "}
            {shardCount > 0 ? `${shardCount} shard${shardCount !== 1 ? "s" : ""}` : "—"} ·{" "}
            term {term !== undefined ? String(term) : "—"}
          </div>
        </div>

        <nav className="nav">
          <div className="nav-section">Cluster</div>
          {NAV.map((n) => (
            <div
              key={n.id}
              className={`nav-item${screen === n.id ? " active" : ""}`}
              onClick={() => setScreen(n.id)}
            >
              {tweaks.showRunes && <span className="nav-glyph">{n.glyph}</span>}
              <span>{n.label}</span>
            </div>
          ))}
          <div className="nav-section">Data plane</div>
          {NAV_DATA.map((n) => (
            <div
              key={n.id}
              className={`nav-item${screen === n.id ? " active" : ""}`}
              onClick={() => setScreen(n.id)}
            >
              {tweaks.showRunes && <span className="nav-glyph">{n.glyph}</span>}
              <span>{n.label}</span>
            </div>
          ))}
          {NAV_DISABLED.map((n) => (
            <div
              key={n.id}
              className="nav-item"
              style={{ opacity: 0.4, cursor: "not-allowed" }}
              title="not yet implemented"
            >
              {tweaks.showRunes && <span className="nav-glyph">{n.glyph}</span>}
              <span>{n.label}</span>
            </div>
          ))}
        </nav>

        <div className="sidebar-foot">
          <span className="dot" />
          <span className="mono">ggap-console</span>
          <span style={{ flex: 1 }} />
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <div className="crumbs">
            <span>local</span>
            <span className="sep">/</span>
            <span className="crumb-now">{crumbName}</span>
          </div>
          <div className="topbar-spacer" />
          <div className="topbar-search">
            <span className="muted">⌕</span>
            <span>Search keys, shards, nodes…</span>
            <kbd>⌘K</kbd>
          </div>
        </header>

        <div className="content" key={screen}>
          {renderScreen()}
        </div>
      </main>

      <Toasts items={toasts} />

      <TweaksPanel title="Tweaks">
        <TweakSection label="Theme">
          <TweakRadio
            label="Theme"
            value={tweaks.theme}
            options={[
              { value: "dark", label: "Void" },
              { value: "dim", label: "Dim" },
              { value: "light", label: "Light" },
            ]}
            onChange={(v) => setTweak("theme", v)}
          />
          <TweakRadio
            label="Density"
            value={tweaks.density}
            options={[
              { value: "compact", label: "Compact" },
              { value: "balanced", label: "Balanced" },
              { value: "cozy", label: "Cozy" },
            ]}
            onChange={(v) => setTweak("density", v)}
          />
        </TweakSection>
        <TweakSection label="Aesthetic">
          <TweakSlider
            label="Accent hue"
            value={tweaks.accentHue}
            min={0}
            max={360}
            step={5}
            unit="°"
            onChange={(v) => setTweak("accentHue", v)}
          />
          <TweakToggle
            label="Mono everywhere"
            value={tweaks.monoEverywhere}
            onChange={(v) => setTweak("monoEverywhere", v)}
          />
          <TweakToggle
            label="Show runes"
            value={tweaks.showRunes}
            onChange={(v) => setTweak("showRunes", v)}
          />
        </TweakSection>
      </TweaksPanel>
    </div>
  );
}
