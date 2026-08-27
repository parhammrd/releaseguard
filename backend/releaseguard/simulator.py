from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4

from .config import Settings
from .models import DemoPhase, HealthCohort, HealthPoint, ReleaseDecision, ServiceMetric
from .state import DemoController


class EventPublisher(Protocol):
    async def publish_metric(self, metric: ServiceMetric) -> None: ...

    async def publish_release_event(self, event: dict) -> None: ...

    async def publish_decision(self, decision: ReleaseDecision) -> None: ...

    async def publish_action(self, action: dict) -> None: ...


class TelemetrySimulator:
    def __init__(self, settings: Settings, controller: DemoController, publisher: EventPublisher) -> None:
        self.settings = settings
        self.controller = controller
        self.publisher = publisher
        self._stop = asyncio.Event()
        self._samples: deque[ServiceMetric] = deque()
        self._rng = random.Random(settings.simulator_seed)
        self._sequence = 0
        self._canary_sequence = 0
        self._stable_sequence = 0
        self._generation = controller.generation()
        self._last_emit = 0.0

    def stop(self) -> None:
        self._stop.set()

    def reset_seed(self) -> None:
        self._rng = random.Random(self.settings.simulator_seed + self.controller.generation())
        self._sequence = 0
        self._canary_sequence = 0
        self._stable_sequence = 0
        self._samples.clear()
        self._generation = self.controller.generation()
        self._last_emit = 0.0

    def generate_metric(self) -> ServiceMetric:
        state = self.controller.snapshot()
        self._sequence += 1
        canary_active = state["phase"] in {
            DemoPhase.CANARY_RUNNING.value,
            DemoPhase.REGRESSION.value,
            DemoPhase.ROLLBACK_PENDING.value,
        }
        is_canary = canary_active and self._routes_to_canary(
            self._sequence,
            state["canary_percent"],
        )
        cohort = "canary" if is_canary else "stable"
        version = self.settings.canary_version if is_canary else self.settings.stable_version
        regression = is_canary and state["regression_enabled"]

        if regression:
            self._canary_sequence += 1
            latency = round(max(410, self._rng.gauss(650, 72)))
            failed = self._canary_sequence % 5 == 0  # One in five canary requests fails.
        elif is_canary:
            self._canary_sequence += 1
            latency = round(max(80, self._rng.gauss(154, 19)))
            failed = self._canary_sequence % 131 == 0  # <1% of canary requests.
        else:
            self._stable_sequence += 1
            latency = round(max(75, self._rng.gauss(149, 17)))
            failed = self._stable_sequence % 170 == 0  # ~0.6% of stable requests.

        return ServiceMetric(
            event_id=f"req_{uuid4().hex[:20]}",
            release_id=state["release_id"],
            service=self.settings.service_name,
            version=version,
            cohort=cohort,
            region="us-east-2",
            latency_ms=latency,
            status_code=503 if failed else 200,
            event_time=int(time.time() * 1000),
        )

    @staticmethod
    def _routes_to_canary(sequence: int, traffic_percent: int) -> bool:
        """Spread an integer percentage evenly across each 100-request cycle."""
        return traffic_percent > 0 and (sequence * traffic_percent) % 100 < traffic_percent

    async def run(self) -> None:
        batch_size = max(1, round(self.settings.events_per_second * self.settings.simulator_interval_seconds))
        while not self._stop.is_set():
            if self._generation != self.controller.generation():
                self.reset_seed()
            started = time.monotonic()
            for _ in range(batch_size):
                metric = self.generate_metric()
                self._samples.append(metric)
                await self.controller.record_metric(metric.cohort, metric.status_code)
                await self.publisher.publish_metric(metric)

            now = time.monotonic()
            if now - self._last_emit >= self.settings.health_emit_seconds:
                self._last_emit = now
                if not self.settings.kafka_enabled:
                    point = self.aggregate_health(source="local")
                    decision = await self.controller.record_health(point)
                    if decision:
                        await self.publisher.publish_decision(decision)
                        if self.settings.local_decisions:
                            await asyncio.sleep(0.65)
                            action = await self.controller.apply_rollback(decision.decision_id)
                            await self.publisher.publish_action(action.model_dump())

            elapsed = time.monotonic() - started
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=max(0.01, self.settings.simulator_interval_seconds - elapsed),
                )
            except TimeoutError:
                pass

    def aggregate_health(self, source: str = "local") -> HealthPoint:
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.settings.health_window_seconds * 1000
        while self._samples and self._samples[0].event_time < cutoff:
            self._samples.popleft()

        def cohort_stats(name: str) -> HealthCohort:
            samples = [sample for sample in self._samples if sample.cohort == name]
            if not samples:
                return HealthCohort()
            errors = sum(sample.status_code >= 500 for sample in samples)
            return HealthCohort(
                request_count=len(samples),
                error_count=errors,
                error_rate=round(errors / len(samples), 4),
                avg_latency_ms=round(sum(sample.latency_ms for sample in samples) / len(samples), 1),
            )

        end = datetime.now(UTC)
        start = end - timedelta(seconds=self.settings.health_window_seconds)
        return HealthPoint(
            release_id=self.controller.release_id,
            window_start=start.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            window_end=end.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            stable=cohort_stats("stable"),
            canary=cohort_stats("canary"),
            source=source,
        )
