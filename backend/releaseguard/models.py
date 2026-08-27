from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class DemoPhase(str, Enum):
    READY = "READY"
    CANARY_RUNNING = "CANARY_RUNNING"
    REGRESSION = "REGRESSION"
    ROLLBACK_PENDING = "ROLLBACK_PENDING"
    ROLLED_BACK = "ROLLED_BACK"


class CanaryRequest(BaseModel):
    version: str = Field(default="v2.4.0", min_length=2, max_length=32)
    traffic_percent: int = Field(default=10, ge=1, le=50)


class RegressionRequest(BaseModel):
    enabled: bool = True


class ServiceMetric(BaseModel):
    event_id: str
    release_id: str
    service: str
    version: str
    cohort: Literal["stable", "canary"]
    region: str
    latency_ms: int
    status_code: int
    event_time: int


class HealthCohort(BaseModel):
    request_count: int = 0
    error_count: int = 0
    error_rate: float = 0.0
    avg_latency_ms: float = 0.0


class HealthPoint(BaseModel):
    release_id: str
    window_start: str
    window_end: str
    stable: HealthCohort
    canary: HealthCohort
    source: Literal["local", "flink"] = "local"


class ReleaseDecision(BaseModel):
    decision_id: str
    release_id: str
    decision: Literal["ROLLBACK"]
    reason_code: str
    reason: str
    stable_error_rate: float
    canary_error_rate: float
    stable_avg_latency_ms: float
    canary_avg_latency_ms: float
    decided_at: str
    source: str = "flink"


class ActionResult(BaseModel):
    action_id: str
    decision_id: str
    release_id: str
    status: Literal["applied", "duplicate", "failed"]
    action: Literal["ROLLBACK_CANARY"] = "ROLLBACK_CANARY"
    previous_canary_percent: int
    canary_percent: int
    applied_at: str
    detail: str


class TimelineEvent(BaseModel):
    event_id: str
    event_type: str
    title: str
    detail: str
    tone: Literal["neutral", "violet", "amber", "red", "green"] = "neutral"
    occurred_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)
