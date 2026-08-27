#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="$PROJECT_DIR/infra/terraform"
CREDENTIAL_FILE="${CONFLUENT_GLOBAL_KEY_FILE:-}"
CLOUD_MODE="${1:-}"

cd "$PROJECT_DIR"

if [[ "$CLOUD_MODE" == "--cloud" ]]; then
  if [[ -z "$CREDENTIAL_FILE" || ! -f "$CREDENTIAL_FILE" ]]; then
    echo "Set CONFLUENT_GLOBAL_KEY_FILE to the bootstrap Global key file." >&2
    exit 1
  fi
  export TF_VAR_confluent_cloud_api_key
  export TF_VAR_confluent_cloud_api_secret
  TF_VAR_confluent_cloud_api_key="$(awk '/^API key:/{getline; print; exit}' "$CREDENTIAL_FILE")"
  TF_VAR_confluent_cloud_api_secret="$(awk '/^API secret:/{getline; print; exit}' "$CREDENTIAL_FILE")"

  terraform -chdir="$TERRAFORM_DIR" init
  terraform -chdir="$TERRAFORM_DIR" apply -auto-approve -var enable_http_sink=false
  python3 "$PROJECT_DIR/scripts/render_runtime_env.py"
elif [[ ! -f .env.local ]]; then
  WEBHOOK_TOKEN="$(openssl rand -hex 24)"
  {
    echo "WEBHOOK_SECRET=$WEBHOOK_TOKEN"
    echo "LOCAL_DECISIONS=true"
  } > .env.local
fi

docker compose up --build -d app tunnel

for _ in {1..30}; do
  if curl --silent --fail http://localhost:8000/healthz >/dev/null; then
    break
  fi
  sleep 1
done
curl --silent --fail http://localhost:8000/healthz >/dev/null

TUNNEL_URL=""
for _ in {1..30}; do
  TUNNEL_URL="$(docker compose logs --no-color tunnel 2>/dev/null | grep -Eo 'https://[-a-z0-9]+\.trycloudflare\.com' | tail -1 || true)"
  [[ -n "$TUNNEL_URL" ]] && break
  sleep 1
done

if [[ "$CLOUD_MODE" == "--cloud" ]]; then
  if [[ -z "$TUNNEL_URL" ]]; then
    echo "Quick Tunnel URL was not discovered; connector was not enabled." >&2
    exit 1
  fi
  WEBHOOK_TOKEN="$(awk -F= '/^WEBHOOK_SECRET=/{print $2; exit}' .env.local)"
  terraform -chdir="$TERRAFORM_DIR" apply -auto-approve \
    -var enable_http_sink=true \
    -var "public_backend_base_url=$TUNNEL_URL" \
    -var "webhook_bearer_token=$WEBHOOK_TOKEN"
fi

echo "ReleaseGuard: http://localhost:8000"
if [[ -n "$TUNNEL_URL" ]]; then
  echo "Webhook tunnel: $TUNNEL_URL (HTTP Sink only; dashboard SSE stays on localhost)"
fi
