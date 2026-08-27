from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from releaseguard.app import app, controller
from releaseguard.config import Settings
from releaseguard.models import HealthCohort, HealthPoint, ReleaseDecision
from releaseguard.simulator import TelemetrySimulator
from releaseguard.state import DemoController


class NoopPublisher:
    async def publish_metric(self, metric):
        return None

    async def publish_release_event(self, event):
        return None

    async def publish_decision(self, decision):
        return None

    async def publish_action(self, action):
        return None


def health(*, stable_count=250, canary_count=25, stable_error=0.008, canary_error=0.20,
           stable_latency=150.0, canary_latency=650.0) -> HealthPoint:
    stamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return HealthPoint(
        release_id="release-test",
        window_start=stamp,
        window_end=stamp,
        stable=HealthCohort(
            request_count=stable_count,
            error_count=round(stable_count * stable_error),
            error_rate=stable_error,
            avg_latency_ms=stable_latency,
        ),
        canary=HealthCohort(
            request_count=canary_count,
            error_count=round(canary_count * canary_error),
            error_rate=canary_error,
            avg_latency_ms=canary_latency,
        ),
    )


@pytest.mark.asyncio
async def test_seeded_telemetry_profiles_are_reproducible() -> None:
    settings = Settings()
    first_controller = DemoController(settings)
    second_controller = DemoController(settings)
    first = TelemetrySimulator(settings, first_controller, NoopPublisher())
    second = TelemetrySimulator(settings, second_controller, NoopPublisher())

    first_values = [(first.generate_metric().latency_ms, first.generate_metric().status_code) for _ in range(50)]
    second_values = [(second.generate_metric().latency_ms, second.generate_metric().status_code) for _ in range(50)]
    assert first_values == second_values

    await first_controller.launch_canary("v2.4.0", 10)
    await first_controller.set_regression(True)
    samples = [first.generate_metric() for _ in range(500)]
    canary = [sample for sample in samples if sample.cohort == "canary"]
    assert len(canary) == 50
    assert sum(sample.status_code >= 500 for sample in canary) / len(canary) == pytest.approx(0.20)
    assert 610 <= sum(sample.latency_ms for sample in canary) / len(canary) <= 690


@pytest.mark.asyncio
async def test_state_transitions_and_breach_policy() -> None:
    demo = DemoController(Settings())
    assert demo.snapshot()["phase"] == "READY"
    await demo.launch_canary("v2.4.0", 10)
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"

    # Healthy and undersampled windows must not roll back.
    await demo.record_health(health(canary_error=0.009, canary_latency=154))
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"
    await demo.set_regression(True)
    assert await demo.record_health(health(stable_count=49, canary_count=9)) is None
    assert demo.snapshot()["phase"] == "REGRESSION"

    decision = await demo.record_health(health())
    assert decision is not None
    assert demo.snapshot()["phase"] == "ROLLBACK_PENDING"
    result = await demo.apply_rollback(decision.decision_id)
    assert result.status == "applied"
    assert demo.snapshot()["phase"] == "ROLLED_BACK"
    assert demo.snapshot()["canary_percent"] == 0
    assert demo.snapshot()["metrics"]["errors_avoided_estimate"] > 0
    assert demo.snapshot()["metrics"]["sessions_protected_estimate"] > 0


@pytest.mark.asyncio
async def test_latency_guardrail_requires_ratio_and_absolute_gap() -> None:
    demo = DemoController(Settings())
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)
    assert await demo.record_health(health(canary_error=0.01, canary_latency=275)) is None
    decision = await demo.record_health(health(canary_error=0.01, canary_latency=310))
    assert decision is not None
    assert decision.reason_code == "LATENCY_REGRESSION"


@pytest.mark.asyncio
async def test_duplicate_decision_is_exactly_once() -> None:
    demo = DemoController(Settings())
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)
    decision = await demo.record_health(health())
    assert decision
    applied = await demo.apply_rollback(decision.decision_id)
    duplicate = await demo.apply_rollback(decision.decision_id)
    assert applied.status == "applied"
    assert duplicate.status == "duplicate"
    assert applied.action_id == duplicate.action_id
    assert demo.snapshot()["canary_percent"] == 0


def test_webhook_authentication_and_retry_handling() -> None:
    with TestClient(app) as client:
        client.post("/api/demo/reset")
        client.post("/api/demo/canary", json={"version": "v2.4.0", "traffic_percent": 10})
        stamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        decision = ReleaseDecision(
            decision_id="dec_webhook_retry_test",
            release_id=controller.release_id,
            decision="ROLLBACK",
            reason_code="ERROR_RATE_REGRESSION",
            reason="Canary errors 20.0% vs stable 0.8%",
            stable_error_rate=0.008,
            canary_error_rate=0.20,
            stable_avg_latency_ms=150,
            canary_avg_latency_ms=650,
            decided_at=stamp,
        )
        route = f"/api/v1/release-decisions/{decision.decision_id}"
        assert client.post(route).status_code == 401
        headers = {"Authorization": "Bearer releaseguard-local-demo-secret"}
        first = client.post(route, headers=headers, json=decision.model_dump())
        retry = client.post(route, headers=headers)
        assert first.status_code == 200
        assert first.json()["status"] == "applied"
        assert retry.status_code == 200
        assert retry.json()["status"] == "duplicate"


def test_health_and_frontend_are_packaged_together() -> None:
    with TestClient(app) as client:
        assert client.get("/healthz").json()["status"] == "ok"
        page = client.get("/")
        assert page.status_code == 200
        assert "ReleaseGuard" in page.text
        assert client.get("/", headers={"Host": "demo.trycloudflare.com"}).status_code == 404
        assert client.get("/healthz", headers={"Host": "demo.trycloudflare.com"}).status_code == 200
