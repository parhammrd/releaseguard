from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

# Unit tests must not inherit ignored developer or challenge cloud credentials.
for credential_name in (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
):
    os.environ[credential_name] = ""
os.environ["LOCAL_DECISIONS"] = "true"
os.environ["WEBHOOK_SECRET"] = "releaseguard-local-demo-secret"

from releaseguard.app import _connector_decision_payload, app, controller
from releaseguard.config import Settings
from releaseguard.kafka_gateway import KafkaGateway, _decision_from_flink
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


class RecordingProducer:
    def __init__(self) -> None:
        self.calls = []

    def produce(self, **kwargs):
        self.calls.append(kwargs)

    def poll(self, timeout):
        return 0


def health(*, release_id="release-test", stable_count=250, canary_count=25,
           stable_error=0.008, canary_error=0.20,
           stable_latency=150.0, canary_latency=650.0) -> HealthPoint:
    stamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return HealthPoint(
        release_id=release_id,
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
async def test_kafka_producer_omits_missing_timestamp() -> None:
    settings = Settings(
        kafka_bootstrap_servers="test:9092",
        kafka_api_key="key",
        kafka_api_secret="secret",
        schema_registry_url="https://schema.test",
        schema_registry_api_key="schema-key",
        schema_registry_api_secret="schema-secret",
    )
    gateway = KafkaGateway(settings, DemoController(settings))
    producer = RecordingProducer()
    gateway._producer = producer
    gateway._serializers = {"metric": lambda value, context: b"encoded"}

    await gateway._produce("releaseguard_test", "key", {}, "metric")
    await gateway._produce("releaseguard_test", "key", {}, "metric", timestamp=1234)

    assert "timestamp" not in producer.calls[0]
    assert producer.calls[1]["timestamp"] == 1234


def test_flink_logical_timestamp_is_normalized() -> None:
    decided_at = datetime(2026, 8, 27, 19, 30, tzinfo=UTC)
    decision = _decision_from_flink(
        {
            "decision_id": "dec_timestamp_test",
            "release_id": "release-test",
            "decision": "ROLLBACK",
            "reason_code": "ERROR_RATE_REGRESSION",
            "reason": "Canary errors exceeded the guardrail",
            "stable_error_rate": 0.008,
            "canary_error_rate": 0.20,
            "stable_avg_latency_ms": 150.0,
            "canary_avg_latency_ms": 650.0,
            "decided_at": decided_at,
        }
    )

    assert decision.decided_at == "2026-08-27T19:30:00Z"
    assert decision.source == "flink"


def test_connector_envelope_and_epoch_timestamp_are_normalized() -> None:
    payload = json.dumps(
        {
            "value": {
                "release_id": "release-test",
                "decision": "ROLLBACK",
                "reason_code": "ERROR_RATE_REGRESSION",
                "reason": "Canary errors exceeded the guardrail",
                "stable_error_rate": 0.008,
                "canary_error_rate": 0.20,
                "stable_avg_latency_ms": 150.0,
                "canary_avg_latency_ms": 650.0,
                "decided_at": 1787859000000,
                "source": "flink",
            }
        }
    )

    candidate = _connector_decision_payload(payload, "dec_connector_test")
    decision = ReleaseDecision.model_validate(candidate)
    byte_decision = ReleaseDecision.model_validate(
        _connector_decision_payload(payload.encode("utf-8"), "dec_connector_bytes_test")
    )

    assert decision.decision_id == "dec_connector_test"
    assert decision.decided_at.endswith("Z")
    assert byte_decision.decision_id == "dec_connector_bytes_test"


def test_clock_skew_cannot_produce_a_negative_duration() -> None:
    assert DemoController._duration(
        "2026-08-27T20:41:10.300Z",
        "2026-08-27T20:41:10.000Z",
    ) == 0.0


@pytest.mark.asyncio
async def test_state_transitions_and_breach_policy() -> None:
    demo = DemoController(Settings())
    assert demo.snapshot()["phase"] == "READY"
    await demo.launch_canary("v2.4.0", 10)
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"

    # Healthy and undersampled windows must not roll back.
    await demo.record_health(health(release_id=demo.release_id, canary_error=0.009, canary_latency=154))
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"
    await demo.set_regression(True)
    assert await demo.record_health(
        health(release_id=demo.release_id, stable_count=49, canary_count=9)
    ) is None
    assert demo.snapshot()["phase"] == "REGRESSION"

    decision = await demo.record_health(health(release_id=demo.release_id))
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
    assert await demo.record_health(
        health(release_id=demo.release_id, canary_error=0.01, canary_latency=275)
    ) is None
    decision = await demo.record_health(
        health(release_id=demo.release_id, canary_error=0.01, canary_latency=310)
    )
    assert decision is not None
    assert decision.reason_code == "LATENCY_REGRESSION"


@pytest.mark.asyncio
async def test_duplicate_decision_is_exactly_once() -> None:
    demo = DemoController(Settings())
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)
    decision = await demo.record_health(health(release_id=demo.release_id))
    assert decision
    applied = await demo.apply_rollback(decision.decision_id)
    duplicate = await demo.apply_rollback(decision.decision_id)
    assert applied.status == "applied"
    assert duplicate.status == "duplicate"
    assert applied.action_id == duplicate.action_id
    assert demo.snapshot()["canary_percent"] == 0


@pytest.mark.asyncio
async def test_previous_run_events_cannot_roll_back_a_new_canary() -> None:
    demo = DemoController(Settings())
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)
    previous_health = health(release_id=demo.release_id)
    previous_decision = await demo.record_health(previous_health)
    assert previous_decision

    await demo.reset()
    await demo.launch_canary("v2.4.0", 10)
    assert await demo.record_health(previous_health) is None
    assert await demo.register_external_decision(previous_decision) is False

    ignored = await demo.apply_rollback(previous_decision.decision_id)
    assert ignored.status == "duplicate"
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"
    assert demo.snapshot()["canary_percent"] == 10


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
