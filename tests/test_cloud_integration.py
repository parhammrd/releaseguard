from __future__ import annotations

import os
import time

import httpx
import pytest


pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.getenv("RUN_CLOUD_TESTS") != "1", reason="Set RUN_CLOUD_TESTS=1 for the live Confluent acceptance run")
def test_healthy_regression_connector_rollback_and_retry() -> None:
    base_url = os.getenv("RELEASEGUARD_BASE_URL", "http://localhost:8000")
    bearer = os.environ["WEBHOOK_SECRET"]
    client = httpx.Client(base_url=base_url, timeout=10)

    reset = client.post("/api/demo/reset")
    reset.raise_for_status()
    launch = client.post("/api/demo/canary", json={"version": "v2.4.0", "traffic_percent": 10})
    launch.raise_for_status()
    time.sleep(4)
    healthy = client.get("/api/demo/state").json()
    assert healthy["phase"] == "CANARY_RUNNING"
    assert healthy["latest_decision"] is None

    injected_at = time.monotonic()
    regression = client.post("/api/demo/regression", json={"enabled": True})
    regression.raise_for_status()
    while time.monotonic() - injected_at < 12:
        final = client.get("/api/demo/state").json()
        if final["phase"] == "ROLLED_BACK":
            break
        time.sleep(0.25)
    else:
        pytest.fail("Confluent decision and HTTP Sink rollback exceeded 12 seconds")

    assert final["pipeline"]["mode"] == "confluent"
    assert final["latest_decision"]["source"] == "flink"
    assert final["canary_percent"] == 0
    decision_id = final["latest_decision"]["decision_id"]
    retry = client.post(
        f"/api/v1/release-decisions/{decision_id}",
        headers={"Authorization": f"Bearer {bearer}"},
    )
    retry.raise_for_status()
    assert retry.json()["status"] == "duplicate"
