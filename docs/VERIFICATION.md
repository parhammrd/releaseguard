# Verification

ReleaseGuard was checked on August 27, 2026 against the local build and the
deployed Confluent Cloud pipeline.

## Local checks

The following commands passed:

- `npm run lint`
- `npm run build`
- `npm run test:browser`: 2 Chromium checks passed
- `.venv/bin/pytest -q`: 20 passed, 1 cloud test skipped
- `terraform -chdir=infra/terraform fmt -check`
- `terraform -chdir=infra/terraform validate`
- `docker compose config --quiet`
- `bash -n scripts/start_demo.sh scripts/teardown_releaseguard.sh`
- `git diff --check`

Five deterministic local rehearsals completed without a rollback during the
healthy period:

```text
run 1: rollback in 2.51s
run 2: rollback in 2.51s
run 3: rollback in 2.52s
run 4: rollback in 2.50s
run 5: rollback in 2.50s
five-run max: 2.52s; false rollbacks: 0
```

This rehearsal checks repeatability of the local scenario; it is not a cloud
latency benchmark.

## Cloud check

The deployed resources are:

| Resource | Name | ID / location |
|---|---|---|
| Environment | `releaseguard` | `env-g2k97n` |
| Standard Kafka cluster | `releaseguard` | `lkc-yoyw1qk` · AWS `us-east-2` |
| Schema Registry | — | `lsrc-w71w3ow` |
| Flink compute pool | `releaseguard_default` | `lfcp-k8komq2` |
| Flink statement | `releaseguard-window-health-v1` | health windows |
| Flink statement | `releaseguard-release-decisions-v1` | rollback decisions |
| Managed connector | `releaseguard_http_sink_v2` | HTTP Sink V2 |

The schemas use `BACKWARD_TRANSITIVE` compatibility. Both Flink statements and
the connector were running at the end of the check.

The cloud acceptance test passed five consecutive runs. Each run kept the
canary healthy for four seconds, waited for a Flink decision, observed HTTP Sink
V2 delivery, and retried the delivered decision without a second traffic
change.

```text
run 1: 7.28s total
run 2: 7.80s total
run 3: 7.53s total
run 4: 7.52s total
run 5: 7.50s total
```

The totals include the four-second healthy period. The slowest run remained
below the 12-second rollback limit after regression injection.

The HTTP Sink uses one-record batches and bounded retries. Exhausted or rejected
records are written to the configured error and DLQ topics without stopping
later records. The webhook still rejects stale releases and mismatched
decision IDs, and suppresses repeated application of an accepted decision.

Automated browser checks cover local/cloud labeling, SSE updates across a
reload, the local rollback flow, and the 390-pixel layout. A separate cloud-mode
browser run verified a 100/0 post-rollback split, Flink and connector evidence,
and an empty browser console. The resulting dashboard is stored in
[`screenshots/releaseguard-rollback.jpg`](screenshots/releaseguard-rollback.jpg).

## Repeat the checks

Run the local suite:

```bash
make test
npx playwright install chromium
npm run test:browser
LOCAL_DECISIONS=true \
KAFKA_BOOTSTRAP_SERVERS= KAFKA_API_KEY= KAFKA_API_SECRET= \
SCHEMA_REGISTRY_URL= SCHEMA_REGISTRY_API_KEY= SCHEMA_REGISTRY_API_SECRET= \
make rehearse
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
docker compose config --quiet
bash -n scripts/start_demo.sh scripts/teardown_releaseguard.sh
```

The empty cloud variables keep the rehearsal local if `.env.local` was last
written by cloud mode.

With the cloud-mode app and connector running, use:

```bash
RUN_CLOUD_TESTS=1 \
WEBHOOK_SECRET="$(awk -F= '/^WEBHOOK_SECRET=/{print $2; exit}' .env.local)" \
.venv/bin/pytest tests/test_cloud_integration.py -q -s
```

The bootstrap credential file, `.env.local`, Terraform state, generated plans,
Python environments, and build output are ignored by Git. `.env.local` and the
local Terraform state are stored with owner-only permissions.
