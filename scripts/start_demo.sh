#!/usr/bin/env bash
set -euo pipefail
umask 077

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="$PROJECT_DIR/infra/terraform"
CREDENTIAL_FILE="${CONFLUENT_GLOBAL_KEY_FILE:-}"
MODE="${1:-}"

cd "$PROJECT_DIR"

if [[ -n "$MODE" && "$MODE" != "--cloud" ]]; then
  echo "Usage: $0 [--cloud]" >&2
  exit 2
fi

wait_for_app() {
  for _ in {1..30}; do
    if curl --silent --fail http://localhost:8000/healthz >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "ReleaseGuard did not become healthy at http://localhost:8000/healthz." >&2
  return 1
}

load_bootstrap_credentials() {
  if [[ -z "$CREDENTIAL_FILE" || ! -f "$CREDENTIAL_FILE" ]]; then
    echo "Set CONFLUENT_GLOBAL_KEY_FILE to the bootstrap Global key file." >&2
    exit 1
  fi

  TF_VAR_confluent_cloud_api_key="$(awk '/^API key:/{getline; sub(/\r$/, ""); print; exit}' "$CREDENTIAL_FILE")"
  TF_VAR_confluent_cloud_api_secret="$(awk '/^API secret:/{getline; sub(/\r$/, ""); print; exit}' "$CREDENTIAL_FILE")"

  if [[ -z "$TF_VAR_confluent_cloud_api_key" || -z "$TF_VAR_confluent_cloud_api_secret" ]]; then
    echo "The credential file must contain API key: and API secret: headings followed by their values." >&2
    exit 1
  fi
  if [[ "$TF_VAR_confluent_cloud_api_key" =~ [[:space:]] || "$TF_VAR_confluent_cloud_api_secret" =~ [[:space:]] ]]; then
    echo "The parsed Confluent credentials contain whitespace; check the credential file format." >&2
    exit 1
  fi
  if (( ${#TF_VAR_confluent_cloud_api_key} < 8 || ${#TF_VAR_confluent_cloud_api_secret} < 16 )); then
    echo "The parsed Confluent credentials are shorter than expected; check the credential file." >&2
    exit 1
  fi

  export TF_VAR_confluent_cloud_api_key
  export TF_VAR_confluent_cloud_api_secret
}

if [[ "$MODE" != "--cloud" ]]; then
  WEBHOOK_TOKEN="$(openssl rand -hex 24)"
  {
    echo "WEBHOOK_SECRET=$WEBHOOK_TOKEN"
    echo "LOCAL_DECISIONS=true"
  } > .env.local
  chmod 600 .env.local

  # A local run must not leave a tunnel from an earlier cloud session running.
  docker compose stop tunnel >/dev/null 2>&1 || true
  docker compose up --build --force-recreate -d app
  wait_for_app

  echo "ReleaseGuard local twin: http://localhost:8000"
  exit 0
fi

load_bootstrap_credentials
export TF_VAR_enable_flink=true
export TF_VAR_enable_http_sink=true

echo "Cloud mode can create or update paid Confluent resources."
echo "Review each Terraform plan before approving it."

terraform -chdir="$TERRAFORM_DIR" init -input=false

# The public connector URL does not exist until the app and tunnel are running.
# Target only the runtime prerequisites here so an existing connector is never
# planned for removal between the bootstrap and final applies.
terraform -chdir="$TERRAFORM_DIR" apply \
  -target=confluent_kafka_topic.releaseguard \
  -target=confluent_subject_config.value \
  -target=confluent_subject_config.decision_key \
  -target=confluent_api_key.runtime_kafka \
  -target=confluent_api_key.runtime_schema_registry

python3 "$PROJECT_DIR/scripts/render_runtime_env.py"
chmod 600 .env.local

docker compose up --build --force-recreate -d app
wait_for_app
docker compose up --force-recreate -d tunnel

TUNNEL_URL=""
for _ in {1..30}; do
  TUNNEL_URL="$(docker compose logs --no-color tunnel 2>/dev/null | grep -Eo 'https://[-a-z0-9]+\.trycloudflare\.com' | tail -1 || true)"
  [[ -n "$TUNNEL_URL" ]] && break
  sleep 1
done

if [[ -z "$TUNNEL_URL" ]]; then
  echo "Quick Tunnel URL was not discovered; Flink and the connector were not changed." >&2
  exit 1
fi

WEBHOOK_TOKEN="$(awk -F= '/^WEBHOOK_SECRET=/{print $2; exit}' .env.local)"
if (( ${#WEBHOOK_TOKEN} < 24 )); then
  echo "WEBHOOK_SECRET is missing or too short in .env.local." >&2
  exit 1
fi

export TF_VAR_public_backend_base_url="$TUNNEL_URL"
export TF_VAR_webhook_bearer_token="$WEBHOOK_TOKEN"

# Flink statements and HTTP Sink V2 are applied together so the existing
# connector is never removed during a cloud relaunch.
terraform -chdir="$TERRAFORM_DIR" apply

echo "ReleaseGuard: http://localhost:8000"
echo "Webhook tunnel: $TUNNEL_URL (HTTP Sink only; dashboard SSE stays on localhost)"
