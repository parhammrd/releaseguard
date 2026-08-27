#!/usr/bin/env bash
set -euo pipefail

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

export TF_VAR_confluent_cloud_api_key
export TF_VAR_confluent_cloud_api_secret
TF_VAR_confluent_cloud_api_key="$(awk '/^API key:/{getline; print; exit}' "$CREDENTIAL_FILE")"
TF_VAR_confluent_cloud_api_secret="$(awk '/^API secret:/{getline; print; exit}' "$CREDENTIAL_FILE")"

terraform -chdir="$TERRAFORM_DIR" destroy
