import type { DemoSnapshot } from "@/app/types";

type StatusTone = "green" | "violet" | "amber" | "red";
type StatusCopy = {
  eyebrow: string;
  title: string;
  body: string;
  tone: StatusTone;
};

const localPhaseCopy: Record<DemoSnapshot["phase"], StatusCopy> = {
  READY: {
    eyebrow: "LOCAL TWIN READY",
    title: "The demo is ready",
    body: "Stable v2.3.4 is receiving all generated traffic. No cloud resources are in use.",
    tone: "green",
  },
  CANARY_RUNNING: {
    eyebrow: "LOCAL CANARY",
    title: "The simulated canary is healthy",
    body: "v2.4.0 is receiving 10% of generated traffic while the local evaluator compares both cohorts.",
    tone: "violet",
  },
  REGRESSION: {
    eyebrow: "LOCAL REGRESSION",
    title: "The simulated canary is degrading",
    body: "Generated canary traffic now has elevated latency and errors. The local evaluator is checking the current window.",
    tone: "amber",
  },
  ROLLBACK_PENDING: {
    eyebrow: "LOCAL GUARDRAIL",
    title: "Local rollback decision emitted",
    body: "The local evaluator emitted ROLLBACK and the in-process action is changing the simulated allocation.",
    tone: "red",
  },
  ROLLED_BACK: {
    eyebrow: "LOCAL ROLLBACK",
    title: "Local rollback completed",
    body: "The simulated canary was isolated and the event feed recorded the action.",
    tone: "green",
  },
};

function getStatus(snapshot: DemoSnapshot, connected: boolean): StatusCopy {
  if (!connected) {
    return {
      eyebrow: "RECONNECTING",
      title: "Event stream interrupted",
      body: "ReleaseGuard is reconnecting to the dashboard event feed.",
      tone: "amber",
    };
  }

  if (snapshot.pipeline.mode !== "confluent") {
    return localPhaseCopy[snapshot.phase];
  }

  const usesFlink = snapshot.pipeline.flink_health_observed;
  const evaluator = snapshot.pipeline.flink_decision_observed
    ? "Flink"
    : "The decision stream";
  const connectorAction = snapshot.pipeline.connector_delivery
    ? "HTTP Sink V2 delivered the control action."
    : "The app is waiting for HTTP delivery.";
  const appliedAction = snapshot.pipeline.connector_delivery
    ? "HTTP Sink V2 delivered the rollback action."
    : "The rollback action was applied.";

  const cloudPhaseCopy: Record<DemoSnapshot["phase"], StatusCopy> = {
    READY: {
      eyebrow: "CONFLUENT MODE CONFIGURED",
      title: "The demo is ready",
      body: "Stable v2.3.4 is receiving all generated traffic. Observed cloud stages will appear as records arrive.",
      tone: "green",
    },
    CANARY_RUNNING: {
      eyebrow: "CANARY OBSERVATION",
      title: "The simulated canary is healthy",
      body: usesFlink
        ? "v2.4.0 is receiving generated canary traffic while Flink compares both cohorts."
        : "v2.4.0 is receiving generated canary traffic; the app is waiting for its first Flink health window.",
      tone: "violet",
    },
    REGRESSION: {
      eyebrow: "REGRESSION STREAMING",
      title: "The simulated canary is degrading",
      body: usesFlink
        ? "Generated canary telemetry shows elevated latency and errors. Flink is evaluating the current window."
        : "Generated canary telemetry shows elevated latency and errors. The app is waiting for a Flink health window.",
      tone: "amber",
    },
    ROLLBACK_PENDING: {
      eyebrow: "GUARDRAIL BREACHED",
      title: "Rollback decision emitted",
      body: `${evaluator} emitted ROLLBACK. ${connectorAction}`,
      tone: "red",
    },
    ROLLED_BACK: {
      eyebrow: "ROLLBACK APPLIED",
      title: "Rollback completed",
      body: `${appliedAction} Generated canary traffic is now at zero percent.`,
      tone: "green",
    },
  };

  return cloudPhaseCopy[snapshot.phase];
}

export function StatusBanner({
  snapshot,
  connected,
}: {
  snapshot: DemoSnapshot;
  connected: boolean;
}) {
  const status = getStatus(snapshot, connected);
  const rolledBack = snapshot.phase === "ROLLED_BACK";

  return (
    <section className={`status-banner tone-${status.tone}`}>
      <div className="status-icon">
        <span>
          {snapshot.phase === "ROLLED_BACK"
            ? "✓"
            : snapshot.phase === "ROLLBACK_PENDING"
              ? "!"
              : "◆"}
        </span>
      </div>
      <div className="status-copy">
        <p>{status.eyebrow}</p>
        <h2>{status.title}</h2>
        <span>{status.body}</span>
      </div>
      <div className="status-proof">
        {rolledBack ? (
          <>
            <strong>
              {snapshot.metrics.sessions_protected_estimate.toLocaleString()}
            </strong>
            <span>estimated sessions spared · demo</span>
          </>
        ) : (
          <>
            <strong>
              {snapshot.stable_percent}/{snapshot.canary_percent}
            </strong>
            <span>simulated stable / canary split</span>
          </>
        )}
      </div>
    </section>
  );
}
