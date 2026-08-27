#!/usr/bin/env bash
set -euo pipefail
umask 077

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="$PROJECT_DIR/infra/terraform"

if [[ "${1:-}" != "--execute" ]]; then
  echo "Scoped teardown is ready but was not run."
  echo "After explicit approval: CONFLUENT_GLOBAL_KEY_FILE=<path> ./scripts/teardown_releaseguard.sh --execute"
  exit 0
fi

CREDENTIAL_FILE="${CONFLUENT_GLOBAL_KEY_FILE:-}"
if [[ -z "$CREDENTIAL_FILE" || ! -f "$CREDENTIAL_FILE" ]]; then
  echo "Set CONFLUENT_GLOBAL_KEY_FILE to the Global key file used for bootstrap." >&2
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

terraform -chdir="$TERRAFORM_DIR" init -input=false
terraform -chdir="$TERRAFORM_DIR" destroy
docker compose down
