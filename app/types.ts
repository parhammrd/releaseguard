export type CohortHealth = {
  request_count: number;
  error_count: number;
  error_rate: number;
  avg_latency_ms: number;
};

export type HealthPoint = {
  window_end: string;
  stable: CohortHealth;
  canary: CohortHealth;
  source: "local" | "flink";
};

export type TimelineEvent = {
  event_id: string;
  event_type: string;
  title: string;
  detail: string;
  tone: "neutral" | "violet" | "amber" | "red" | "green";
  occurred_at: string;
};

export type DemoSnapshot = {
  release_id: string;
  service: string;
  phase:
    | "READY"
    | "CANARY_RUNNING"
    | "REGRESSION"
    | "ROLLBACK_PENDING"
    | "ROLLED_BACK";
  stable_version: string;
  canary_version: string;
  stable_percent: number;
  canary_percent: number;
  started_at: string | null;
  latest_decision: null | {
    decision_id: string;
    reason: string;
    reason_code: string;
    canary_error_rate: number;
    canary_avg_latency_ms: number;
  };
  latest_action: null | {
    decision_id: string;
    detail: string;
  };
  health_history: HealthPoint[];
  timeline: TimelineEvent[];
  metrics: {
    events_per_second: number;
    total_requests: number;
    canary_requests: number;
    canary_errors: number;
    time_to_detect_seconds: number | null;
    recovery_time_seconds: number | null;
    errors_avoided_estimate: number;
    sessions_protected_estimate: number;
    estimate_baseline_seconds: number;
  };
  pipeline: {
    mode: "confluent" | "local_preview";
    configured_mode: "confluent" | "local_preview";
    health_source: "not_observed" | "flink" | "deterministic_local_twin";
    flink_health_observed: boolean;
    flink_decision_observed: boolean;
    connector_delivery: boolean;
  };
};
