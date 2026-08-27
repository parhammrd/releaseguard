import { useMemo } from "react";
import type { HealthPoint } from "@/app/types";

function formatTime(value: string) {
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function MiniBars({
  points,
  metric,
}: {
  points: HealthPoint[];
  metric: "latency" | "errors";
}) {
  const values = points.slice(-18);
  const ceiling = metric === "latency" ? 750 : 25;

  return (
    <div
      className="chart-shell"
      aria-label={`${metric} history for stable and canary releases`}
    >
      <div className="chart-grid" />
      {metric === "errors" && (
        <div className="slo-line">
          <span>5% guardrail</span>
        </div>
      )}
      <div className="chart-bars">
        {values.length === 0 && (
          <p className="chart-empty">Waiting for the first streaming window…</p>
        )}
        {values.map((point, index) => {
          const stableValue =
            metric === "latency"
              ? point.stable.avg_latency_ms
              : point.stable.error_rate * 100;
          const canaryValue =
            metric === "latency"
              ? point.canary.avg_latency_ms
              : point.canary.error_rate * 100;
          const stableHeight = Math.max(
            stableValue > 0 ? 3 : 0,
            Math.min(100, (stableValue / ceiling) * 100),
          );
          const canaryHeight = Math.max(
            canaryValue > 0 ? 3 : 0,
            Math.min(100, (canaryValue / ceiling) * 100),
          );
          const breached = canaryValue > (metric === "latency" ? 400 : 5);

          return (
            <div
              className="bar-slot"
              key={`${point.window_end}-${index}`}
              title={`${formatTime(point.window_end)} · stable ${stableValue.toFixed(1)} · canary ${canaryValue.toFixed(1)}`}
            >
              <i
                className="bar stable-bar"
                style={{ height: `${stableHeight}%` }}
              />
              <i
                className={`bar canary-bar ${breached ? "breach" : ""}`}
                style={{ height: `${canaryHeight}%` }}
              />
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function HealthChart({
  points,
  cloudMode,
  stableVersion,
  canaryVersion,
  rolledBack,
}: {
  points: HealthPoint[];
  cloudMode: boolean;
  stableVersion: string;
  canaryVersion: string;
  rolledBack: boolean;
}) {
  const latest = points.at(-1);
  const canaryPeakLatency = useMemo(
    () => Math.max(0, ...points.map((point) => point.canary.avg_latency_ms)),
    [points],
  );
  const canaryPeakErrors = useMemo(
    () => Math.max(0, ...points.map((point) => point.canary.error_rate * 100)),
    [points],
  );
  return (
    <section className="panel health-card">
      <div className="panel-head chart-heading">
        <div>
          <p className="eyebrow">
            {cloudMode ? "STREAMED DEMO HEALTH" : "GENERATED DEMO HEALTH"}
          </p>
          <h3>Stable versus canary</h3>
          <span className="subcopy">
            Six-second hopping windows · updates every two seconds
          </span>
        </div>
        <div className="legend">
          <span>
            <i className="stable-dot" />
            {stableVersion}
          </span>
          <span>
            <i className="canary-dot" />
            {canaryVersion}
          </span>
        </div>
      </div>
      <div className="chart-grid-pair">
        <div className="chart-block">
          <div className="chart-label">
            <span>Average latency</span>
            <b>
              {latest?.canary.avg_latency_ms
                ? `${latest.canary.avg_latency_ms.toFixed(0)} ms canary`
                : "No canary traffic"}
            </b>
          </div>
          <MiniBars points={points} metric="latency" />
          <div className="chart-scale">
            <span>0</span>
            <span>750 ms</span>
          </div>
        </div>
        <div className="chart-block">
          <div className="chart-label">
            <span>Error rate</span>
            <b>
              {latest?.canary.request_count
                ? `${(latest.canary.error_rate * 100).toFixed(1)}% canary`
                : "No canary traffic"}
            </b>
          </div>
          <MiniBars points={points} metric="errors" />
          <div className="chart-scale">
            <span>0</span>
            <span>25%</span>
          </div>
        </div>
      </div>
      <div className="health-summary">
        <span>
          <b>{latest ? latest.stable.avg_latency_ms.toFixed(0) : "—"} ms</b>{" "}
          stable now
        </span>
        <span>
          <b>{canaryPeakLatency.toFixed(0)} ms</b> canary peak
        </span>
        <span>
          <b>{canaryPeakErrors.toFixed(1)}%</b> error peak
        </span>
        <span className={rolledBack ? "safe" : ""}>
          <b>{rolledBack ? "ISOLATED" : "MONITORING"}</b> guardrail
        </span>
      </div>
    </section>
  );
}
