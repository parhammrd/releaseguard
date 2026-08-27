import type { DemoSnapshot } from "@/app/types";

function formatTime(value: string | null) {
  if (!value) return "—";

  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function ReleaseOverview({ snapshot }: { snapshot: DemoSnapshot }) {
  return (
    <section className="panel release-card">
      <div className="panel-head">
        <div>
          <p className="eyebrow">
            SIMULATED SERVICE · {snapshot.service.toUpperCase()}
          </p>
          <h3>Release {snapshot.canary_version}</h3>
        </div>
        <span className={`phase-pill phase-${snapshot.phase.toLowerCase()}`}>
          {snapshot.phase.replaceAll("_", " ")}
        </span>
      </div>
      <div className="release-lanes">
        <div className="release-lane stable-lane">
          <span>STABLE</span>
          <strong>{snapshot.stable_version}</strong>
          <b>{snapshot.stable_percent}% traffic</b>
        </div>
        <div className="lane-arrow">
          <i />
          <span>versus</span>
          <i />
        </div>
        <div className="release-lane canary-lane">
          <span>CANARY</span>
          <strong>{snapshot.canary_version}</strong>
          <b>{snapshot.canary_percent}% traffic</b>
        </div>
      </div>
      <div className="traffic-caption">
        <span>Simulated traffic allocation</span>
        <b>{snapshot.stable_percent}% stable</b>
      </div>
      <div className="traffic-bar">
        <i style={{ width: `${snapshot.stable_percent}%` }} />
        <b style={{ width: `${snapshot.canary_percent}%` }} />
      </div>
      <div className="release-footer">
        <span>{snapshot.release_id}</span>
        <span>
          {snapshot.started_at
            ? `Started ${formatTime(snapshot.started_at)}`
            : "Ready to launch"}
        </span>
      </div>
    </section>
  );
}
