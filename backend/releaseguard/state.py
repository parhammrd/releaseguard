from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .config import Settings
from .models import ActionResult, DemoPhase, HealthPoint, ReleaseDecision, TimelineEvent


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventHub:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    async def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    async def publish(self, event_type: str, data: dict[str, Any]) -> None:
        envelope = {"type": event_type, "data": data}
        for queue in tuple(self._subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(envelope)


class DemoController:
    """Owns the demo state machine; Kafka transports facts but never owns UI state."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.hub = EventHub()
        self._lock = asyncio.Lock()
        self._generation = 0
        self._processed_decisions: dict[str, ActionResult] = {}
        self._known_decisions: dict[str, ReleaseDecision] = {}
        self.health_history: deque[dict[str, Any]] = deque(maxlen=60)
        self.timeline: deque[dict[str, Any]] = deque(maxlen=40)
        self._initialize()

    def _initialize(self) -> None:
        self._generation += 1
        self.release_id = f"release-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S-%f')}"
        self.phase = DemoPhase.READY
        self.canary_percent = 0
        self.regression_enabled = False
        self.started_at: str | None = None
        self.regression_at: str | None = None
        self.decision_at: str | None = None
        self.rollback_at: str | None = None
        self.latest_decision: dict[str, Any] | None = None
        self.latest_action: dict[str, Any] | None = None
        self._canary_allocation_percent = 0
        self._observed_health_source: str | None = None
        self._flink_health_observed = False
        self._flink_decision_observed = False
        self._connector_delivery_observed = False
        self.total_requests = 0
        self.canary_requests = 0
        self.canary_errors = 0
        self.regression_canary_requests = 0
        self.regression_canary_errors = 0
        self.health_history.clear()
        self.timeline.clear()
        self._processed_decisions.clear()
        self._known_decisions.clear()
        self._add_timeline(
            "system_ready",
            "Stable baseline healthy",
            f"{self.settings.stable_version} is serving all simulated requests",
            "green",
        )

    def _add_timeline(
        self,
        event_type: str,
        title: str,
        detail: str,
        tone: str = "neutral",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = TimelineEvent(
            event_id=f"evt_{uuid4().hex[:12]}",
            event_type=event_type,
            title=title,
            detail=detail,
            tone=tone,
            occurred_at=now_iso(),
            metadata=metadata or {},
        ).model_dump()
        self.timeline.append(event)
        return event

    def snapshot(self) -> dict[str, Any]:
        time_to_detect = self._duration(self.regression_at, self.decision_at)
        recovery_time = self._duration(self.decision_at, self.rollback_at)
        # Scenario estimates compare automation with a five-minute manual response.
        remaining_manual_seconds = max(0.0, 300.0 - (time_to_detect or 0.0) - (recovery_time or 0.0))
        projected_canary_requests = (
            remaining_manual_seconds
            * self.settings.events_per_second
            * self._canary_allocation_percent
            / 100
        )
        error_delta = max(0.0, 0.20 - 0.008)
        errors_avoided = round(projected_canary_requests * error_delta)
        sessions_protected = round(projected_canary_requests)
        return {
            "release_id": self.release_id,
            "service": self.settings.service_name,
            "phase": self.phase.value,
            "stable_version": self.settings.stable_version,
            "canary_version": self.settings.canary_version,
            "stable_percent": 100 - self.canary_percent,
            "canary_percent": self.canary_percent,
            "regression_enabled": self.regression_enabled,
            "started_at": self.started_at,
            "regression_at": self.regression_at,
            "decision_at": self.decision_at,
            "rollback_at": self.rollback_at,
            "latest_decision": self.latest_decision,
            "latest_action": self.latest_action,
            "health_history": list(self.health_history),
            "timeline": list(self.timeline),
            "metrics": {
                "events_per_second": self.settings.events_per_second,
                "total_requests": self.total_requests,
                "canary_requests": self.canary_requests,
                "canary_errors": self.canary_errors,
                "time_to_detect_seconds": time_to_detect,
                "recovery_time_seconds": recovery_time,
                "errors_avoided_estimate": errors_avoided if self.rollback_at else 0,
                "sessions_protected_estimate": sessions_protected if self.rollback_at else 0,
                "estimate_baseline_seconds": 300,
            },
            "pipeline": {
                "mode": "confluent" if self.settings.kafka_enabled else "local_preview",
                "configured_mode": "confluent" if self.settings.kafka_enabled else "local_preview",
                "health_source": self._observed_health_source or "not_observed",
                "flink_health_observed": self._flink_health_observed,
                "flink_decision_observed": self._flink_decision_observed,
                "connector_delivery": self._connector_delivery_observed,
            },
        }

    @staticmethod
    def _duration(start: str | None, end: str | None) -> float | None:
        if not start or not end:
            return None
        started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        finished = datetime.fromisoformat(end.replace("Z", "+00:00"))
        # Flink and the local webhook can differ by a few hundred milliseconds.
        # A duration is never meaningfully negative, so keep clock skew out of
        # the impact cards while preserving the original event timestamps.
        return round(max(0.0, (finished - started).total_seconds()), 1)

    async def _broadcast_snapshot(self) -> None:
        await self.hub.publish("snapshot", self.snapshot())

    async def reset(self) -> dict[str, Any]:
        async with self._lock:
            self._initialize()
            snapshot = self.snapshot()
        await self.hub.publish("snapshot", snapshot)
        return snapshot

    async def launch_canary(self, version: str, traffic_percent: int) -> tuple[dict[str, Any], dict[str, Any]]:
        async with self._lock:
            if self.phase not in {DemoPhase.READY, DemoPhase.ROLLED_BACK}:
                raise ValueError("A canary is already active")
            if version != self.settings.canary_version:
                raise ValueError(f"This scenario is pinned to {self.settings.canary_version}")
            if self.phase is DemoPhase.ROLLED_BACK:
                self._initialize()
            self.phase = DemoPhase.CANARY_RUNNING
            self.canary_percent = traffic_percent
            self._canary_allocation_percent = traffic_percent
            self.started_at = now_iso()
            event = self._add_timeline(
                "canary_launched",
                f"Canary {version} launched",
                f"{traffic_percent}% of simulated requests",
                "violet",
                {"traffic_percent": traffic_percent, "version": version},
            )
            snapshot = self.snapshot()
        await self.hub.publish("action", event)
        await self.hub.publish("snapshot", snapshot)
        return snapshot, event

    async def set_regression(self, enabled: bool) -> tuple[dict[str, Any], dict[str, Any]]:
        async with self._lock:
            if enabled and self.phase is not DemoPhase.CANARY_RUNNING:
                raise ValueError("Launch the canary before injecting a regression")
            if not enabled and self.phase not in {DemoPhase.CANARY_RUNNING, DemoPhase.REGRESSION}:
                raise ValueError("There is no active canary regression")
            self.regression_enabled = enabled
            self.phase = DemoPhase.REGRESSION if enabled else DemoPhase.CANARY_RUNNING
            self.regression_at = now_iso() if enabled else None
            event = self._add_timeline(
                "regression_injected" if enabled else "regression_cleared",
                "Regression injected" if enabled else "Regression cleared",
                "Canary latency ≈650 ms · failures ≈20%" if enabled else "Canary health restored",
                "amber" if enabled else "green",
            )
            snapshot = self.snapshot()
        await self.hub.publish("action", event)
        await self.hub.publish("snapshot", snapshot)
        return snapshot, event

    async def record_metric(self, cohort: str, status_code: int) -> None:
        async with self._lock:
            self.total_requests += 1
            if cohort == "canary":
                self.canary_requests += 1
                if status_code >= 500:
                    self.canary_errors += 1
                if self.regression_enabled:
                    self.regression_canary_requests += 1
                    if status_code >= 500:
                        self.regression_canary_errors += 1

    async def record_health(self, point: HealthPoint) -> ReleaseDecision | None:
        decision: ReleaseDecision | None = None
        async with self._lock:
            if point.release_id != self.release_id:
                return None
            payload = point.model_dump()
            self.health_history.append(payload)
            self._observed_health_source = (
                "flink" if point.source == "flink" else "deterministic_local_twin"
            )
            if point.source == "flink":
                self._flink_health_observed = True
            local_evaluator_enabled = (
                self.settings.local_decisions
                and not self.settings.kafka_enabled
                and point.source == "local"
            )
            if (
                local_evaluator_enabled
                and self.phase is DemoPhase.REGRESSION
                and self._threshold_breached(point)
            ):
                decision = self._make_decision(point)
                self._known_decisions[decision.decision_id] = decision
                self.latest_decision = decision.model_dump()
                self.phase = DemoPhase.ROLLBACK_PENDING
                self.decision_at = decision.decided_at
                self._add_timeline(
                    "rollback_decision",
                    "Local evaluator emitted ROLLBACK",
                    decision.reason,
                    "red",
                    {"decision_id": decision.decision_id},
                )
            snapshot = self.snapshot()
        await self.hub.publish("window_health", payload)
        if decision:
            await self.hub.publish("decision", decision.model_dump())
        await self.hub.publish("snapshot", snapshot)
        return decision

    @staticmethod
    def _threshold_breached(point: HealthPoint) -> bool:
        stable = point.stable
        canary = point.canary
        if stable.request_count < 50 or canary.request_count < 10:
            return False
        stable_error_floor = max(stable.error_rate, 0.001)
        error_breach = (
            canary.error_rate - stable.error_rate >= 0.05
            and canary.error_rate >= 2 * stable_error_floor
        )
        latency_breach = (
            canary.avg_latency_ms >= 1.75 * stable.avg_latency_ms
            and canary.avg_latency_ms - stable.avg_latency_ms >= 150
        )
        return error_breach or latency_breach

    def _make_decision(self, point: HealthPoint) -> ReleaseDecision:
        error_breach = point.canary.error_rate - point.stable.error_rate >= 0.05
        reason_code = "ERROR_RATE_REGRESSION" if error_breach else "LATENCY_REGRESSION"
        if error_breach:
            reason = (
                f"Canary errors {point.canary.error_rate * 100:.1f}% vs "
                f"stable {point.stable.error_rate * 100:.1f}%"
            )
        else:
            reason = (
                f"Canary latency {point.canary.avg_latency_ms:.0f} ms vs "
                f"stable {point.stable.avg_latency_ms:.0f} ms"
            )
        return ReleaseDecision(
            decision_id=f"dec_{uuid4().hex[:16]}",
            release_id=self.release_id,
            decision="ROLLBACK",
            reason_code=reason_code,
            reason=reason,
            stable_error_rate=point.stable.error_rate,
            canary_error_rate=point.canary.error_rate,
            stable_avg_latency_ms=point.stable.avg_latency_ms,
            canary_avg_latency_ms=point.canary.avg_latency_ms,
            decided_at=now_iso(),
            source="flink" if point.source == "flink" else "local-flink-twin",
        )

    async def register_external_decision(self, decision: ReleaseDecision) -> bool:
        async with self._lock:
            existing = self._known_decisions.get(decision.decision_id)
            if existing is not None:
                same_decision = existing.model_dump() == decision.model_dump()
                accepted = same_decision and (
                    decision.decision_id in self._processed_decisions
                    or (
                        decision.release_id == self.release_id
                        and self.canary_percent > 0
                        and self.phase is DemoPhase.ROLLBACK_PENDING
                    )
                )
                snapshot = self.snapshot()
                is_new = False
            else:
                matches_current_release = (
                    decision.release_id == self.release_id
                    and decision.source == "flink"
                )
                starts_rollback = (
                    matches_current_release
                    and self.canary_percent > 0
                    and self.phase in {DemoPhase.CANARY_RUNNING, DemoPhase.REGRESSION}
                )
                follows_existing_rollback = matches_current_release and self.phase in {
                    DemoPhase.ROLLBACK_PENDING,
                    DemoPhase.ROLLED_BACK,
                }
                accepted = starts_rollback or follows_existing_rollback
                is_new = starts_rollback
                if accepted:
                    # Flink can emit another qualifying decision in each hopping
                    # window. Remember it so HTTP Sink receives a 2xx duplicate
                    # response instead of stopping on an expected later window.
                    self._known_decisions[decision.decision_id] = decision
            if is_new:
                self.latest_decision = decision.model_dump()
                self.phase = DemoPhase.ROLLBACK_PENDING
                self.decision_at = decision.decided_at
                self._flink_decision_observed = decision.source == "flink"
                self._add_timeline(
                    "rollback_decision",
                    "Flink emitted ROLLBACK",
                    decision.reason,
                    "red",
                    {"decision_id": decision.decision_id},
                )
                snapshot = self.snapshot()
        if is_new:
            await self.hub.publish("decision", decision.model_dump())
            await self.hub.publish("snapshot", snapshot)
        return accepted

    async def apply_rollback(self, decision_id: str, *, via_connector: bool = False) -> ActionResult:
        async with self._lock:
            existing = self._processed_decisions.get(decision_id)
            if existing:
                if via_connector:
                    self._connector_delivery_observed = True
                return ActionResult(
                    **{
                        **existing.model_dump(),
                        "status": "duplicate",
                        "detail": "Decision was already applied; no second traffic change was made.",
                    }
                )
            decision = self._known_decisions.get(decision_id)
            if not decision:
                raise KeyError("Unknown decision_id")
            if (
                decision.release_id != self.release_id
                or self.canary_percent <= 0
                or self.phase is not DemoPhase.ROLLBACK_PENDING
            ):
                result = ActionResult(
                    action_id=f"act_{uuid4().hex[:16]}",
                    decision_id=decision_id,
                    release_id=decision.release_id,
                    status="duplicate",
                    previous_canary_percent=self.canary_percent,
                    canary_percent=self.canary_percent,
                    applied_at=now_iso(),
                    detail="Decision belongs to an inactive or previous demo run; no traffic change was made.",
                )
                self._processed_decisions[decision_id] = result
                return result
            if via_connector:
                self._connector_delivery_observed = True
            previous = self.canary_percent
            self.canary_percent = 0
            self.regression_enabled = False
            self.phase = DemoPhase.ROLLED_BACK
            self.rollback_at = now_iso()
            result = ActionResult(
                action_id=f"act_{uuid4().hex[:16]}",
                decision_id=decision_id,
                release_id=self.release_id,
                status="applied",
                previous_canary_percent=previous,
                canary_percent=0,
                applied_at=self.rollback_at,
                detail=f"Canary traffic shifted from {previous}% to 0%.",
            )
            self._processed_decisions[decision_id] = result
            self.latest_action = result.model_dump()
            event = self._add_timeline(
                "rollback_applied",
                "Rollback completed",
                f"Canary traffic {previous}% → 0%",
                "green",
                {"decision_id": decision_id, "action_id": result.action_id},
            )
            snapshot = self.snapshot()
        await self.hub.publish("action", {**event, "result": result.model_dump()})
        await self.hub.publish("snapshot", snapshot)
        return result

    def generation(self) -> int:
        return self._generation
