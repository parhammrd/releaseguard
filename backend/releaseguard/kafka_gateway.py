from __future__ import annotations

import asyncio
import json
from typing import Any

from .config import PROJECT_ROOT, Settings
from .models import HealthCohort, HealthPoint, ReleaseDecision, ServiceMetric
from .state import DemoController

try:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
except ImportError:  # pragma: no cover - makes local source inspection friendly before install.
    Consumer = Producer = SchemaRegistryClient = None  # type: ignore[assignment]
    AvroDeserializer = AvroSerializer = None  # type: ignore[assignment]
    MessageField = SerializationContext = None  # type: ignore[assignment]


SCHEMA_DIR = PROJECT_ROOT / "infra" / "schemas"


def _schema(name: str) -> str:
    return (SCHEMA_DIR / name).read_text()


class KafkaGateway:
    """Thin optional Confluent boundary; the complete UI remains runnable without credentials."""

    def __init__(self, settings: Settings, controller: DemoController) -> None:
        self.settings = settings
        self.controller = controller
        self.enabled = settings.kafka_enabled
        self._producer: Any = None
        self._consumer: Any = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._serializers: dict[str, Any] = {}

    async def start(self) -> None:
        if not self.enabled:
            return
        if Producer is None:
            raise RuntimeError("Install backend/requirements.txt to enable Confluent mode")
        sr = SchemaRegistryClient(
            {
                "url": self.settings.schema_registry_url,
                "basic.auth.user.info": (
                    f"{self.settings.schema_registry_api_key}:{self.settings.schema_registry_api_secret}"
                ),
            }
        )
        serializer_conf = {"auto.register.schemas": False, "use.latest.version": True}
        self._serializers = {
            "metric": AvroSerializer(sr, _schema("releaseguard_service_metrics.avsc"), conf=serializer_conf),
            "release": AvroSerializer(sr, _schema("releaseguard_release_events.avsc"), conf=serializer_conf),
            "decision": AvroSerializer(sr, _schema("releaseguard_release_decisions.avsc"), conf=serializer_conf),
            "action": AvroSerializer(sr, _schema("releaseguard_action_results.avsc"), conf=serializer_conf),
            "decision_key": AvroSerializer(sr, _schema("releaseguard_release_decisions_key.avsc"), conf=serializer_conf),
        }
        common = {
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.settings.kafka_api_key,
            "sasl.password": self.settings.kafka_api_secret,
        }
        self._producer = Producer(common)
        self._consumer = Consumer(
            {
                **common,
                "group.id": self.settings.consumer_group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "isolation.level": "read_uncommitted",
            }
        )
        self._consumer.subscribe([self.settings.health_topic, self.settings.decisions_topic])
        self._consumer_task = asyncio.create_task(self._consume_loop(sr), name="releaseguard-kafka-consumer")

    async def close(self) -> None:
        self._stop.set()
        if self._consumer_task:
            await self._consumer_task
        if self._consumer:
            self._consumer.close()
        if self._producer:
            self._producer.flush(10)

    async def _produce(self, topic: str, key: str, value: dict[str, Any], serializer_name: str, timestamp: int | None = None) -> None:
        if not self.enabled:
            return
        value_bytes = self._serializers[serializer_name](
            value,
            SerializationContext(topic, MessageField.VALUE),
        )
        if topic == self.settings.decisions_topic:
            key_bytes = self._serializers["decision_key"](
                key,
                SerializationContext(topic, MessageField.KEY),
            )
        else:
            key_bytes = key.encode("utf-8")
        produce_args = {
            "topic": topic,
            "key": key_bytes,
            "value": value_bytes,
            "on_delivery": lambda error, message: None,
        }
        if timestamp is not None:
            produce_args["timestamp"] = timestamp
        self._producer.produce(**produce_args)
        self._producer.poll(0)

    async def publish_metric(self, metric: ServiceMetric) -> None:
        await self._produce(
            self.settings.metrics_topic,
            f"{metric.service}:{metric.release_id}:{metric.cohort}",
            metric.model_dump(),
            "metric",
            timestamp=metric.event_time,
        )

    async def publish_release_event(self, event: dict[str, Any]) -> None:
        payload = {
            "event_id": event.get("event_id", "unknown"),
            "release_id": self.controller.release_id,
            "event_type": event.get("event_type", "unknown"),
            "version": self.settings.canary_version,
            "traffic_percent": int(event.get("metadata", {}).get("traffic_percent", self.controller.canary_percent)),
            "occurred_at": int(__import__("time").time() * 1000),
            "detail": event.get("detail", ""),
        }
        await self._produce(self.settings.release_events_topic, self.controller.release_id, payload, "release")

    async def publish_decision(self, decision: ReleaseDecision) -> None:
        await self._produce(
            self.settings.decisions_topic,
            decision.decision_id,
            _decision_for_avro(decision.model_dump()),
            "decision",
        )

    async def publish_action(self, action: dict[str, Any]) -> None:
        payload = dict(action)
        payload["applied_at"] = _epoch_ms(payload["applied_at"])
        await self._produce(self.settings.actions_topic, payload["decision_id"], payload, "action")

    async def _consume_loop(self, sr: Any) -> None:
        avro = AvroDeserializer(sr)
        while not self._stop.is_set():
            message = await asyncio.to_thread(self._consumer.poll, 0.25)
            if message is None or message.error():
                continue
            try:
                payload = avro(
                    message.value(),
                    SerializationContext(message.topic(), MessageField.VALUE),
                )
                if message.topic() == self.settings.health_topic:
                    await self.controller.record_health(_health_from_flink(payload))
                elif message.topic() == self.settings.decisions_topic:
                    await self.controller.register_external_decision(_decision_from_flink(payload))
            except Exception as exc:  # keep the demo feed alive and expose the issue in logs.
                print(f"ReleaseGuard consumer skipped malformed record: {exc}", flush=True)


def _epoch_ms(value: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _decision_for_avro(payload: dict[str, Any]) -> dict[str, Any]:
    copied = dict(payload)
    copied["decided_at"] = _epoch_ms(copied["decided_at"])
    return copied


def _health_from_flink(payload: dict[str, Any]) -> HealthPoint:
    from datetime import UTC, datetime

    def iso(value: Any) -> str:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000, UTC).isoformat().replace("+00:00", "Z")
        return str(value).replace(" ", "T").replace("+00:00", "Z")

    return HealthPoint(
        release_id=payload["release_id"],
        window_start=iso(payload["window_start"]),
        window_end=iso(payload["window_end"]),
        stable=HealthCohort(
            request_count=int(payload["stable_request_count"]),
            error_count=int(payload["stable_error_count"]),
            error_rate=float(payload["stable_error_rate"]),
            avg_latency_ms=float(payload["stable_avg_latency_ms"]),
        ),
        canary=HealthCohort(
            request_count=int(payload["canary_request_count"]),
            error_count=int(payload["canary_error_count"]),
            error_rate=float(payload["canary_error_rate"]),
            avg_latency_ms=float(payload["canary_avg_latency_ms"]),
        ),
        source="flink",
    )


def _decision_from_flink(payload: dict[str, Any]) -> ReleaseDecision:
    from datetime import UTC, datetime

    decided = payload.get("decided_at")
    if isinstance(decided, datetime):
        if decided.tzinfo is None:
            decided = decided.replace(tzinfo=UTC)
        decided = decided.astimezone(UTC).isoformat().replace("+00:00", "Z")
    elif isinstance(decided, (int, float)):
        decided = datetime.fromtimestamp(decided / 1000, UTC).isoformat().replace("+00:00", "Z")
    return ReleaseDecision(**{**payload, "decided_at": decided, "source": "flink"})
