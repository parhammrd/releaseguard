import type { DemoSnapshot } from "@/app/types";

function formatSeconds(value: number | null) {
  return value === null ? "—" : `${value.toFixed(1)}s`;
}

function Metric({
  label,
  value,
  detail,
  accent,
}: {
  label: string;
  value: string;
  detail: string;
  accent?: boolean;
}) {
  return (
    <div className={`metric ${accent ? "metric-accent" : ""}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      <span>{detail}</span>
    </div>
  );
}

export function ImpactMetrics({ snapshot }: { snapshot: DemoSnapshot }) {
  const rolledBack = snapshot.phase === "ROLLED_BACK";

  return (
    <section className="metric-strip">
      <Metric
        label="TRAFFIC"
        value={`${snapshot.stable_percent} / ${snapshot.canary_percent}`}
        detail="Simulated stable / canary"
      />
      <Metric
        label="DETECT"
        value={formatSeconds(snapshot.metrics.time_to_detect_seconds)}
        detail="Regression to decision"
        accent={rolledBack}
      />
      <Metric
        label="RECOVER"
        value={formatSeconds(snapshot.metrics.recovery_time_seconds)}
        detail="Decision to isolation"
      />
      <Metric
        label="ERRORS AVOIDED"
        value={snapshot.metrics.errors_avoided_estimate.toLocaleString()}
        detail="Demo estimate · 5 min"
      />
      <Metric
        label="SESSIONS SPARED"
        value={snapshot.metrics.sessions_protected_estimate.toLocaleString()}
        detail="Demo estimate · 5 min"
        accent={rolledBack}
      />
    </section>
  );
}
