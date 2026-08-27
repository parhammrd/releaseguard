import type { DemoSnapshot } from "@/app/types";

type DemoAction = (path: string, body?: object) => Promise<void>;

export function DemoControls({
  snapshot,
  connected,
  busy,
  message,
  act,
}: {
  snapshot: DemoSnapshot;
  connected: boolean;
  busy: boolean;
  message: string;
  act: DemoAction;
}) {
  const canLaunch =
    snapshot.phase === "READY" || snapshot.phase === "ROLLED_BACK";
  const canInject = snapshot.phase === "CANARY_RUNNING";
  const cloudMode = snapshot.pipeline.mode === "confluent";

  return (
    <section className="panel controls-card">
      <div>
        <p className="eyebrow">DEMO SCENARIO</p>
        <h3>Controlled checkout regression</h3>
        <span className="subcopy">
          Launch the simulated canary, inject a fault, and follow the guardrail
          decision.
        </span>
      </div>
      <div className="controls">
        <button
          className="primary"
          disabled={!connected || busy || !canLaunch}
          onClick={() =>
            act("/api/demo/canary", { version: "v2.4.0", traffic_percent: 10 })
          }
        >
          {snapshot.phase === "ROLLED_BACK" ? "Replay canary" : "Launch canary"}
          <span>→</span>
        </button>
        <button
          className="danger"
          disabled={!connected || busy || !canInject}
          onClick={() => act("/api/demo/regression", { enabled: true })}
        >
          Inject regression<span>↗</span>
        </button>
        <button
          className="ghost"
          disabled={!connected || busy}
          onClick={() => act("/api/demo/reset")}
        >
          Reset demo
        </button>
      </div>
      {message && <p className="form-error">{message}</p>}
      <p className="simulation-note">
        {cloudMode
          ? `Request traffic and impact figures are simulated. ${snapshot.pipeline.flink_health_observed ? "Flink health has been observed" : "The app is waiting for a Flink health window"}; ${snapshot.pipeline.connector_delivery ? "HTTP Sink V2 delivery has been observed" : "HTTP delivery has not been observed yet"}.`
          : "This run uses the deterministic local twin. No Confluent resources or public tunnel are in use."}
      </p>
    </section>
  );
}
