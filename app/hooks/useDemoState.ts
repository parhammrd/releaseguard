"use client";

import { useCallback, useEffect, useState } from "react";
import type { DemoSnapshot } from "@/app/types";

const emptySnapshot: DemoSnapshot = {
  release_id: "release-initializing",
  service: "checkout-api",
  phase: "READY",
  stable_version: "v2.3.4",
  canary_version: "v2.4.0",
  stable_percent: 100,
  canary_percent: 0,
  started_at: null,
  latest_decision: null,
  latest_action: null,
  health_history: [],
  timeline: [],
  metrics: {
    events_per_second: 50,
    total_requests: 0,
    canary_requests: 0,
    canary_errors: 0,
    time_to_detect_seconds: null,
    recovery_time_seconds: null,
    errors_avoided_estimate: 0,
    sessions_protected_estimate: 0,
    estimate_baseline_seconds: 300,
  },
  pipeline: {
    mode: "local_preview",
    configured_mode: "local_preview",
    health_source: "not_observed",
    flink_health_observed: false,
    flink_decision_observed: false,
    connector_delivery: false,
  },
};

export function useDemoState() {
  const [snapshot, setSnapshot] = useState<DemoSnapshot>(emptySnapshot);
  const [connected, setConnected] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  useEffect(() => {
    const source = new EventSource("/api/events");

    source.onopen = () => setConnected(true);
    source.addEventListener("snapshot", (event) => {
      setSnapshot(JSON.parse((event as MessageEvent).data));
      setConnected(true);
    });
    source.onerror = () => setConnected(false);

    return () => source.close();
  }, []);

  const act = useCallback(async (path: string, body?: object) => {
    setBusy(true);
    setMessage("");

    try {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
      const payload = (await response.json()) as DemoSnapshot & {
        detail?: string;
      };

      if (!response.ok) {
        throw new Error(payload.detail || "The action could not be completed");
      }

      setSnapshot(payload);
    } catch (error) {
      setMessage(
        error instanceof Error
          ? error.message
          : "The action could not be completed",
      );
    } finally {
      setBusy(false);
    }
  }, []);

  return { snapshot, connected, busy, message, act };
}
