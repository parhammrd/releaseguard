import type { DemoSnapshot } from "@/app/types";

export function PipelineOverview({ snapshot }: { snapshot: DemoSnapshot }) {
  const cloudMode = snapshot.pipeline.mode === "confluent";
  const stages = cloudMode
    ? [
        "Kafka configured",
        snapshot.pipeline.flink_health_observed
          ? "Flink health observed"
          : "Awaiting Flink health",
        snapshot.pipeline.flink_decision_observed
          ? "Flink decision observed"
          : "Awaiting decision",
        snapshot.pipeline.connector_delivery
          ? "HTTP Sink V2 delivered"
          : "Awaiting HTTP delivery",
        "Recent events",
      ]
    : [
        "Telemetry generator",
        "Window evaluator",
        "Decision policy",
        "In-process action",
        "Event feed",
      ];

  return (
    <section className="panel products-card">
      <p className="eyebrow">
        {cloudMode ? "CONFIGURED PIPELINE" : "LOCAL TWIN PIPELINE"}
      </p>
      <h3>One path from simulated signal to action</h3>
      <div className="product-flow">
        {stages.map((name, index) => (
          <div key={name}>
            <span>{index + 1}</span>
            <strong>{name}</strong>
          </div>
        ))}
      </div>
      <p className="product-copy">
        {cloudMode
          ? "Cloud mode is configured. Flink and HTTP delivery are marked only after their records reach this app."
          : "The local twin uses the same demo thresholds and state transitions without sending events to Confluent."}
      </p>
    </section>
  );
}
