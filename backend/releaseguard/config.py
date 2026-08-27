from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env.local")


def _bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = "ReleaseGuard"
    service_name: str = "checkout-api"
    stable_version: str = "v2.3.4"
    canary_version: str = "v2.4.0"
    simulator_seed: int = int(os.getenv("SIMULATOR_SEED", "240827"))
    events_per_second: int = int(os.getenv("EVENTS_PER_SECOND", "50"))
    simulator_interval_seconds: float = float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "0.2"))
    health_window_seconds: int = int(os.getenv("HEALTH_WINDOW_SECONDS", "6"))
    health_emit_seconds: int = int(os.getenv("HEALTH_EMIT_SECONDS", "2"))
    local_decisions: bool = _bool("LOCAL_DECISIONS", True)
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "releaseguard-local-demo-secret")
    frontend_dist: Path = Path(os.getenv("FRONTEND_DIST", str(PROJECT_ROOT / "out")))

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    kafka_api_key: str = os.getenv("KAFKA_API_KEY", "")
    kafka_api_secret: str = os.getenv("KAFKA_API_SECRET", "")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "")
    schema_registry_api_key: str = os.getenv("SCHEMA_REGISTRY_API_KEY", "")
    schema_registry_api_secret: str = os.getenv("SCHEMA_REGISTRY_API_SECRET", "")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "releaseguard_dashboard_v1")

    metrics_topic: str = "releaseguard_service_metrics"
    release_events_topic: str = "releaseguard_release_events"
    health_topic: str = "releaseguard_window_health"
    decisions_topic: str = "releaseguard_release_decisions"
    actions_topic: str = "releaseguard_action_results"

    @property
    def kafka_enabled(self) -> bool:
        return all(
            (
                self.kafka_bootstrap_servers,
                self.kafka_api_key,
                self.kafka_api_secret,
                self.schema_registry_url,
                self.schema_registry_api_key,
                self.schema_registry_api_secret,
            )
        )
