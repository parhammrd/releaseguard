from __future__ import annotations

import json
import secrets
import subprocess
from pathlib import Path


root = Path(__file__).resolve().parents[1]
terraform_dir = root / "infra" / "terraform"
result = subprocess.run(
    ["terraform", f"-chdir={terraform_dir}", "output", "-json", "runtime"],
    check=True,
    capture_output=True,
    text=True,
)
runtime = json.loads(result.stdout)
webhook = secrets.token_urlsafe(36)
lines = [
    f"KAFKA_BOOTSTRAP_SERVERS={runtime['kafka_bootstrap_servers']}",
    f"KAFKA_API_KEY={runtime['kafka_api_key']}",
    f"KAFKA_API_SECRET={runtime['kafka_api_secret']}",
    f"SCHEMA_REGISTRY_URL={runtime['schema_registry_url']}",
    f"SCHEMA_REGISTRY_API_KEY={runtime['schema_registry_api_key']}",
    f"SCHEMA_REGISTRY_API_SECRET={runtime['schema_registry_api_secret']}",
    "KAFKA_CONSUMER_GROUP=releaseguard_dashboard_v1",
    f"WEBHOOK_SECRET={webhook}",
    "LOCAL_DECISIONS=false",
]
(root / ".env.local").write_text("\n".join(lines) + "\n")
print("Wrote .env.local with resource-scoped credentials (values not displayed).")
