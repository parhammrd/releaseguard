"use client";

import { DecisionPath } from "@/app/components/DecisionPath";
import { DemoControls } from "@/app/components/DemoControls";
import { HealthChart } from "@/app/components/HealthChart";
import { ImpactMetrics } from "@/app/components/ImpactMetrics";
import { PipelineOverview } from "@/app/components/PipelineOverview";
import { RecentEventTimeline } from "@/app/components/RecentEventTimeline";
import { ReleaseOverview } from "@/app/components/ReleaseOverview";
import { StatusBanner } from "@/app/components/StatusBanner";
import { useDemoState } from "@/app/hooks/useDemoState";

export default function Home() {
  const { snapshot, connected, busy, message, act } = useDemoState();
  const cloudMode = snapshot.pipeline.mode === "confluent";

  return (
    <main>
      <div className="ambient ambient-one" />
      <div className="ambient ambient-two" />
      <div className="dashboard">
        <header className="topbar">
          <div className="brand-group">
            <div className="brand-mark">
              <span>R</span>
              <i />
            </div>
            <div>
              <h1>ReleaseGuard</h1>
              <p>Streaming canary safety demo</p>
            </div>
          </div>
          <div className="header-stats">
            <span className="rate">
              <b>{snapshot.metrics.events_per_second * 60}</b> generated
              events/min
            </span>
            <span className={`live-pill ${connected ? "" : "offline"}`}>
              <i />
              Dashboard events · {connected ? "CONNECTED" : "RECONNECTING"}
            </span>
          </div>
        </header>

        <StatusBanner snapshot={snapshot} connected={connected} />
        <ImpactMetrics snapshot={snapshot} />

        <div className="main-grid">
          <ReleaseOverview snapshot={snapshot} />
          <DemoControls
            snapshot={snapshot}
            connected={connected}
            busy={busy}
            message={message}
            act={act}
          />
          <HealthChart
            points={snapshot.health_history}
            cloudMode={cloudMode}
            stableVersion={snapshot.stable_version}
            canaryVersion={snapshot.canary_version}
            rolledBack={snapshot.phase === "ROLLED_BACK"}
          />
          <DecisionPath snapshot={snapshot} />
          <RecentEventTimeline
            events={snapshot.timeline}
            cloudMode={cloudMode}
          />
          <PipelineOverview snapshot={snapshot} />
        </div>

        <footer>
          <span>ReleaseGuard · deterministic demo scenario</span>
          <span>
            {cloudMode
              ? "Confluent cloud mode · configured"
              : "Local twin · no cloud resources in use"}
          </span>
        </footer>
      </div>
    </main>
  );
}
