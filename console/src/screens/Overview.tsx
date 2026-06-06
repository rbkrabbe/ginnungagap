import React from "react";

const RUNE = "ᚷ";

export default function OverviewScreen() {
  return (
    <div className="fade-in">
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="glyph">{RUNE}</span>
            Overview
          </h1>
          <p className="page-sub">Cluster-wide metrics and activity — coming soon</p>
        </div>
      </div>

      <div
        className="panel"
        style={{
          padding: 48,
          textAlign: "center",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 14,
          color: "var(--fg-3)",
        }}
      >
        <span style={{ fontSize: 36, lineHeight: 1, fontFamily: "var(--font-runic)" }}>{RUNE}</span>
        <div style={{ fontSize: 15, fontWeight: 600, color: "var(--fg-2)" }}>Overview screen is not yet implemented</div>
        <p style={{ maxWidth: 420, margin: 0, fontSize: 13, lineHeight: 1.6 }}>
          This screen will show cluster-wide QPS, latency histograms, status-code distribution, shard
          health, and recent activity once a Prometheus scrape endpoint is wired up. For now, use the
          individual <strong>Nodes</strong>, <strong>Shards</strong>, and <strong>Membership</strong> screens.
        </p>
      </div>
    </div>
  );
}
