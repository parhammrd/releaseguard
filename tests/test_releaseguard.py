from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from releaseguard.app import (
    _connector_decision_payload,
    _mount_frontend,
    _resolve_frontend_file,
    create_app,
)
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


class StubGateway(NoopPublisher):
    def __init__(self) -> None:
        self.start_calls = 0
        self.close_calls = 0
        self.release_events = []
        self.actions = []

    async def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def publish_release_event(self, event):
        self.release_events.append(event)

    async def publish_action(self, action):
        self.actions.append(action)


class IdleSimulator:
    def __init__(self) -> None:
        self.stop_calls = 0
        self.reset_calls = 0

    async def run(self) -> None:
        await asyncio.Event().wait()

    def stop(self) -> None:
        self.stop_calls += 1

    def reset_seed(self) -> None:
        self.reset_calls += 1


@dataclass
class AppHarness:
    app: FastAPI
    controller: DemoController
    gateway: StubGateway
    simulator: IdleSimulator


def local_settings(frontend_dist: Path | None = None) -> Settings:
    return Settings(
        simulator_seed=240827,
        events_per_second=50,
        simulator_interval_seconds=0.2,
        health_window_seconds=6,
        health_emit_seconds=2,
        local_decisions=True,
        webhook_secret="releaseguard-local-demo-secret",
        frontend_dist=frontend_dist or Path(__file__).resolve().parents[1] / "out",
        kafka_bootstrap_servers="",
        kafka_api_key="",
        kafka_api_secret="",
        schema_registry_url="",
        schema_registry_api_key="",
        schema_registry_api_secret="",
        consumer_group="releaseguard_test",
    )


@pytest.fixture
def local_app() -> AppHarness:
    settings = local_settings()
    controller = DemoController(settings)
    gateway = StubGateway()
    simulator = IdleSimulator()
    application = create_app(
        settings,
        controller=controller,
        gateway=gateway,
        simulator=simulator,
    )
    return AppHarness(application, controller, gateway, simulator)


class RecordingProducer:
    def __init__(self) -> None:
        self.calls = []

    def produce(self, **kwargs):
        self.calls.append(kwargs)

    def poll(self, timeout):
        return 0


def health(*, release_id="release-test", stable_count=250, canary_count=25,
           stable_error=0.008, canary_error=0.20,
           stable_latency=150.0, canary_latency=650.0,
           source="local") -> HealthPoint:
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
        source=source,
    )


def release_decision(release_id: str, decision_id: str = "dec_test") -> ReleaseDecision:
    return ReleaseDecision(
        decision_id=decision_id,
        release_id=release_id,
        decision="ROLLBACK",
        reason_code="ERROR_RATE_REGRESSION",
        reason="Canary errors 20.0% vs stable 0.8%",
        stable_error_rate=0.008,
        canary_error_rate=0.20,
        stable_avg_latency_ms=150,
        canary_avg_latency_ms=650,
        decided_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        source="flink",
    )


@pytest.mark.asyncio
async def test_seeded_telemetry_profiles_are_reproducible() -> None:
    settings = local_settings()
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
@pytest.mark.parametrize("traffic_percent", [1, 5, 10, 25, 33, 50])
async def test_simulator_respects_requested_canary_percentage(traffic_percent: int) -> None:
    settings = local_settings()
    demo = DemoController(settings)
    await demo.launch_canary("v2.4.0", traffic_percent)
    simulator = TelemetrySimulator(settings, demo, NoopPublisher())

    samples = [simulator.generate_metric() for _ in range(1_000)]

    assert sum(sample.cohort == "canary" for sample in samples) == traffic_percent * 10


@pytest.mark.asyncio
async def test_kafka_producer_omits_missing_timestamp() -> None:
    settings = replace(
        local_settings(),
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
    mismatched = json.loads(payload)
    mismatched["value"]["decision_id"] = "dec_body"
    with pytest.raises(ValueError, match="does not match"):
        _connector_decision_payload(mismatched, "dec_url")


def test_clock_skew_cannot_produce_a_negative_duration() -> None:
    assert DemoController._duration(
        "2026-08-27T20:41:10.300Z",
        "2026-08-27T20:41:10.000Z",
    ) == 0.0


@pytest.mark.asyncio
async def test_state_transitions_and_breach_policy() -> None:
    demo = DemoController(local_settings())
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
    assert demo.snapshot()["pipeline"]["health_source"] == "deterministic_local_twin"
    assert demo.snapshot()["pipeline"]["connector_delivery"] is False


@pytest.mark.asyncio
async def test_latency_guardrail_requires_ratio_and_absolute_gap() -> None:
    demo = DemoController(local_settings())
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
async def test_duplicate_decision_applies_rollback_once() -> None:
    demo = DemoController(local_settings())
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
    demo = DemoController(local_settings())
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)
    previous_health = health(release_id=demo.release_id)
    previous_decision = await demo.record_health(previous_health)
    assert previous_decision

    await demo.reset()
    await demo.launch_canary("v2.4.0", 10)
    assert await demo.record_health(previous_health) is None
    assert await demo.register_external_decision(previous_decision) is False

    with pytest.raises(KeyError):
        await demo.apply_rollback(previous_decision.decision_id)
    assert demo.snapshot()["phase"] == "CANARY_RUNNING"
    assert demo.snapshot()["canary_percent"] == 10


@pytest.mark.asyncio
async def test_cloud_health_is_observed_but_cannot_create_a_decision() -> None:
    cloud = replace(
        local_settings(),
        kafka_bootstrap_servers="test:9092",
        kafka_api_key="key",
        kafka_api_secret="secret",
        schema_registry_url="https://schema.test",
        schema_registry_api_key="schema-key",
        schema_registry_api_secret="schema-secret",
        local_decisions=True,
    )
    demo = DemoController(cloud)
    assert demo.snapshot()["pipeline"] == {
        "mode": "confluent",
        "configured_mode": "confluent",
        "health_source": "not_observed",
        "flink_health_observed": False,
        "flink_decision_observed": False,
        "connector_delivery": False,
    }
    await demo.launch_canary("v2.4.0", 10)
    await demo.set_regression(True)

    assert await demo.record_health(
        health(release_id=demo.release_id, source="flink")
    ) is None
    observed = demo.snapshot()
    assert observed["phase"] == "REGRESSION"
    assert observed["latest_decision"] is None
    assert observed["pipeline"]["health_source"] == "flink"
    assert observed["pipeline"]["flink_health_observed"] is True

    decision = release_decision(demo.release_id, "dec_flink_authoritative")
    assert await demo.register_external_decision(decision) is True
    assert await demo.register_external_decision(decision) is True
    decision_events = [
        event for event in demo.snapshot()["timeline"]
        if event["event_type"] == "rollback_decision"
    ]
    assert len(decision_events) == 1
    assert demo.snapshot()["pipeline"]["flink_decision_observed"] is True

    await demo.apply_rollback(decision.decision_id)
    assert demo.snapshot()["pipeline"]["connector_delivery"] is False


def test_cloud_configured_app_starts_with_injected_dependencies(tmp_path: Path) -> None:
    cloud = replace(
        local_settings(tmp_path / "unused-frontend"),
        local_decisions=False,
        webhook_secret="cloud-smoke-secret",
        kafka_bootstrap_servers="broker.test:9092",
        kafka_api_key="runtime-key",
        kafka_api_secret="runtime-secret",
        schema_registry_url="https://schema.test",
        schema_registry_api_key="schema-key",
        schema_registry_api_secret="schema-secret",
    )
    controller = DemoController(cloud)
    gateway = StubGateway()
    simulator = IdleSimulator()
    cloud_app = create_app(
        cloud,
        controller=controller,
        gateway=gateway,
        simulator=simulator,
        serve_frontend=False,
    )

    assert cloud_app.state.settings is cloud
    assert cloud_app.state.controller is controller
    with TestClient(cloud_app) as client:
        assert client.get("/healthz").json() == {
            "status": "ok",
            "service": "ReleaseGuard",
            "kafka": "configured",
        }
        snapshot = client.get("/api/demo/state").json()
        assert snapshot["pipeline"]["configured_mode"] == "confluent"
        assert snapshot["pipeline"]["health_source"] == "not_observed"
        assert snapshot["pipeline"]["connector_delivery"] is False

    assert gateway.start_calls == 1
    assert gateway.close_calls == 1
    assert simulator.stop_calls == 1


def test_webhook_authentication_and_retry_handling(local_app: AppHarness) -> None:
    controller = local_app.controller
    with TestClient(local_app.app) as client:
        client.post("/api/demo/reset")
        inactive = release_decision(controller.release_id, "dec_inactive")
        headers = {"Authorization": "Bearer releaseguard-local-demo-secret"}
        rejected = client.post(
            f"/api/v1/release-decisions/{inactive.decision_id}",
            headers=headers,
            json=inactive.model_dump(),
        )
        assert rejected.status_code == 409

        client.post("/api/demo/canary", json={"version": "v2.4.0", "traffic_percent": 10})
        decision = release_decision(controller.release_id, "dec_webhook_retry_test")
        route = f"/api/v1/release-decisions/{decision.decision_id}"
        assert client.post(route).status_code == 401

        mismatched = decision.model_copy(update={"decision_id": "dec_body_mismatch"})
        mismatch = client.post(route, headers=headers, json=mismatched.model_dump())
        assert mismatch.status_code == 422

        stale = decision.model_copy(update={"release_id": "release-previous"})
        stale_response = client.post(route, headers=headers, json=stale.model_dump())
        assert stale_response.status_code == 409
        assert client.get("/api/demo/state").json()["canary_percent"] == 10

        first = client.post(route, headers=headers, json=decision.model_dump())
        retry = client.post(route, headers=headers)
        assert first.status_code == 200
        assert first.json()["status"] == "applied"
        assert retry.status_code == 200
        assert retry.json()["status"] == "duplicate"

        later_window = release_decision(
            controller.release_id,
            "dec_webhook_later_window",
        )
        later = client.post(
            f"/api/v1/release-decisions/{later_window.decision_id}",
            headers=headers,
            json=later_window.model_dump(),
        )
        assert later.status_code == 200
        assert later.json()["status"] == "duplicate"

        snapshot = client.get("/api/demo/state").json()
        assert snapshot["pipeline"]["connector_delivery"] is True
        assert snapshot["pipeline"]["flink_decision_observed"] is True
        assert len(
            [
                event for event in snapshot["timeline"]
                if event["event_type"] == "rollback_decision"
                and event["metadata"].get("decision_id") == decision.decision_id
            ]
        ) == 1


def test_health_and_frontend_are_packaged_together(local_app: AppHarness) -> None:
    with TestClient(local_app.app) as client:
        assert client.get("/healthz").json()["status"] == "ok"
        page = client.get("/")
        assert page.status_code == 200
        assert "ReleaseGuard" in page.text
        missing_api = client.get("/api/not-a-route")
        assert missing_api.status_code == 404
        assert "ReleaseGuard" not in missing_api.text
        assert client.get("/", headers={"Host": "demo.trycloudflare.com"}).status_code == 404
        assert client.get("/healthz", headers={"Host": "demo.trycloudflare.com"}).status_code == 200
    assert local_app.gateway.start_calls == 1
    assert local_app.gateway.close_calls == 1
    assert local_app.simulator.stop_calls == 1


def test_frontend_export_cannot_escape_its_root(tmp_path: Path) -> None:
    dist = tmp_path / "out"
    assets = dist / "_next"
    dist.mkdir()
    assets.mkdir()
    (dist / "index.html").write_text("<h1>Safe frontend</h1>")
    (assets / "app.js").write_text("console.log('safe')")
    secret = tmp_path / "secret.txt"
    secret.write_text("outside export")
    (dist / "escape.txt").symlink_to(secret)
    (assets / "escape.js").symlink_to(secret)

    with pytest.raises(ValueError, match="leaves the export"):
        _resolve_frontend_file(dist, "../secret.txt")
    with pytest.raises(ValueError, match="leaves the export"):
        _resolve_frontend_file(dist, "%252e%252e%252fsecret.txt")
    with pytest.raises(ValueError, match="leaves the export"):
        _resolve_frontend_file(dist, "escape.txt")

    frontend_app = FastAPI()
    _mount_frontend(frontend_app, dist)
    with TestClient(frontend_app) as client:
        assert client.get("/").text == "<h1>Safe frontend</h1>"
        assert client.get("/product/releases").text == "<h1>Safe frontend</h1>"
        assert client.get("/api/not-a-route").status_code == 404
        assert client.get("/%252e%252e%252fsecret.txt").status_code == 404
        assert client.get("/escape.txt").status_code == 404
        assert client.get("/_next/escape.js").status_code == 404
