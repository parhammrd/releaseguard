import type { DemoSnapshot } from "@/app/types";

export function DecisionPath({ snapshot }: { snapshot: DemoSnapshot }) {
  const cloudMode = snapshot.pipeline.mode === "confluent";
  const usesFlink = snapshot.pipeline.flink_health_observed;
  const usesConnector = snapshot.pipeline.connector_delivery;
  const steps = [
    [
      "1",
      "Request telemetry",
      `${snapshot.metrics.total_requests.toLocaleString()} simulated events observed`,
      snapshot.metrics.total_requests > 0,
    ],
    [
      "2",
      cloudMode ? "Confluent Kafka" : "Local telemetry",
      cloudMode
        ? "releaseguard_service_metrics · configured"
        : "Deterministic in-process stream",
      true,
    ],
    [
      "3",
      usesFlink
        ? "Flink SQL"
        : cloudMode
          ? "Awaiting Flink health"
          : "Local window evaluator",
      snapshot.latest_decision?.reason || "Comparing 6-second windows",
      usesFlink,
    ],
    [
      "4",
      "Release decision",
      snapshot.latest_decision ? "ROLLBACK" : "Awaiting guardrail breach",
      Boolean(snapshot.latest_decision),
    ],
    [
      "5",
      usesConnector
        ? "HTTP Sink V2"
        : cloudMode
          ? "Awaiting HTTP Sink V2"
          : "In-process action",
      snapshot.latest_action
        ? "Canary 10% → 0%"
        : "Waiting for a rollback decision",
      Boolean(snapshot.latest_action),
    ],
  ] as const;

  return (
    <section className="panel decision-card">
      <div>
        <p className="eyebrow">DECISION PATH</p>
        <h3>Trace the simulated rollout decision</h3>
      </div>
      <div className="decision-path">
        {steps.map(([number, title, detail, done], index) => (
          <div className={`decision-step ${done ? "done" : ""}`} key={title}>
            {index < steps.length - 1 && <i className="step-line" />}
            <span className="step-number">{done ? "✓" : number}</span>
            <div>
              <strong>{title}</strong>
              <p>{detail}</p>
            </div>
          </div>
        ))}
      </div>
      <div className="decision-id">
        <span>
          {snapshot.latest_decision?.decision_id || "No decision yet"}
        </span>
        <b>{cloudMode ? "CONFLUENT" : "LOCAL TWIN"}</b>
      </div>
    </section>
  );
}
