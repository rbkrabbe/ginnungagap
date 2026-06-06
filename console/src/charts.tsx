import React, { useEffect, useRef, useState } from "react";

export function Sparkline({
  data,
  color = "var(--spark)",
  width = 120,
  height = 32,
  fill = true,
  strokeWidth = 1.5,
}: {
  data: number[];
  color?: string;
  width?: number;
  height?: number;
  fill?: boolean;
  strokeWidth?: number;
}) {
  if (!data || data.length === 0) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const stepX = width / (data.length - 1);
  const points = data.map((v, i) => [i * stepX, height - ((v - min) / range) * height] as const);
  const path = points.map((p, i) => (i === 0 ? "M" : "L") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  const area = path + ` L ${width} ${height} L 0 ${height} Z`;
  return (
    <svg className="chart" width={width} height={height} viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      {fill && <path d={area} fill={color} className="chart-area" />}
      <path d={path} stroke={color} strokeWidth={strokeWidth} fill="none" />
    </svg>
  );
}

export function LineChart({
  series,
  height = 200,
  yFmt = (v: number) => v.toFixed(1),
  padding = { l: 36, r: 12, t: 8, b: 18 },
}: {
  series: { name: string; color: string; data: number[]; strokeWidth?: number }[];
  height?: number;
  yLabel?: string;
  yFmt?: (v: number) => string;
  padding?: { l: number; r: number; t: number; b: number };
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(720);
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) setWidth(e.contentRect.width);
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  const allVals = series.flatMap((s) => s.data);
  const min = 0;
  const max = Math.max(...allVals) * 1.12 || 1;
  const w = width;
  const innerW = w - padding.l - padding.r;
  const innerH = height - padding.t - padding.b;
  const len = series[0]?.data.length || 1;
  const stepX = innerW / Math.max(1, len - 1);

  const yTicks = 4;
  const ticks = Array.from({ length: yTicks + 1 }, (_, i) => min + (max - min) * (i / yTicks));

  function pathFor(data: number[]) {
    return data
      .map((v, i) => {
        const x = padding.l + i * stepX;
        const y = padding.t + innerH - ((v - min) / (max - min)) * innerH;
        return (i === 0 ? "M" : "L") + x.toFixed(1) + " " + y.toFixed(1);
      })
      .join(" ");
  }

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
      <svg width={w} height={height} className="chart">
        <g className="chart-grid">
          {ticks.map((_, i) => {
            const y = padding.t + innerH - (i / yTicks) * innerH;
            return <line key={i} x1={padding.l} x2={w - padding.r} y1={y} y2={y} />;
          })}
        </g>
        <g className="chart-axis">
          {ticks.map((t, i) => {
            const y = padding.t + innerH - (i / yTicks) * innerH;
            return (
              <text key={i} x={padding.l - 6} y={y + 3} textAnchor="end">
                {yFmt(t)}
              </text>
            );
          })}
        </g>
        {series.map((s, i) => (
          <path key={i} d={pathFor(s.data)} stroke={s.color} className="chart-line" strokeWidth={s.strokeWidth || 1.6} />
        ))}
      </svg>
    </div>
  );
}

export function Donut({
  segments,
  size = 96,
  thickness = 12,
  label,
  sublabel,
}: {
  segments: { value: number; color: string }[];
  size?: number;
  thickness?: number;
  label?: React.ReactNode;
  sublabel?: string;
}) {
  const total = segments.reduce((a, s) => a + s.value, 0);
  const r = (size - thickness) / 2;
  const C = 2 * Math.PI * r;
  let acc = 0;
  return (
    <div style={{ position: "relative", width: size, height: size }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} style={{ transform: "rotate(-90deg)" }}>
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="var(--bg-sunken)" strokeWidth={thickness} />
        {segments.map((s, i) => {
          const frac = total === 0 ? 0 : s.value / total;
          const dash = frac * C;
          const offset = -acc;
          acc += dash;
          return (
            <circle
              key={i}
              cx={size / 2}
              cy={size / 2}
              r={r}
              fill="none"
              stroke={s.color}
              strokeWidth={thickness}
              strokeDasharray={`${dash} ${C - dash}`}
              strokeDashoffset={offset}
            />
          );
        })}
      </svg>
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center",
        }}
      >
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500 }}>{label}</div>
        <div style={{ fontSize: 10, color: "var(--fg-3)", letterSpacing: "0.06em", textTransform: "uppercase" }}>{sublabel}</div>
      </div>
    </div>
  );
}
