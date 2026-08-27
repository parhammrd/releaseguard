data "confluent_organization" "current" {}

data "confluent_environment" "target" {
  id = var.environment_id
}

resource "confluent_kafka_cluster" "target" {
  display_name = "releaseguard"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region
  standard {}
  environment { id = data.confluent_environment.target.id }
}

resource "confluent_flink_compute_pool" "target" {
  display_name = "releaseguard_default"
  cloud        = confluent_kafka_cluster.target.cloud
  region       = confluent_kafka_cluster.target.region
  max_cfu      = var.flink_max_cfu
  environment { id = data.confluent_environment.target.id }
}

data "confluent_schema_registry_cluster" "target" {
  environment { id = var.environment_id }
  depends_on = [confluent_kafka_cluster.target]
}

data "confluent_flink_region" "target" {
  cloud  = confluent_kafka_cluster.target.cloud
  region = confluent_kafka_cluster.target.region
}

locals {
  app_topics = {
    releaseguard_service_metrics   = { partitions = 1, retention = "86400000" }
    releaseguard_release_events    = { partitions = 1, retention = "604800000" }
    releaseguard_window_health     = { partitions = 1, retention = "86400000" }
    releaseguard_release_decisions = { partitions = 1, retention = "604800000" }
    releaseguard_action_results    = { partitions = 1, retention = "604800000" }
  }
  operational_topics = {
    releaseguard_http_success = { partitions = 1, retention = "86400000" }
    releaseguard_http_error   = { partitions = 1, retention = "604800000" }
    releaseguard_http_dlq     = { partitions = 1, retention = "604800000" }
  }
  all_topics = merge(local.app_topics, local.operational_topics)
  value_schemas = {
    releaseguard_service_metrics   = "releaseguard_service_metrics.avsc"
    releaseguard_release_events    = "releaseguard_release_events.avsc"
    releaseguard_window_health     = "releaseguard_window_health.avsc"
    releaseguard_release_decisions = "releaseguard_release_decisions.avsc"
    releaseguard_action_results    = "releaseguard_action_results.avsc"
  }
}

# The Global key creates these identities and scoped keys only. Runtime code never receives it.
resource "confluent_service_account" "manager" {
  display_name = "releaseguard_manager"
  description  = "Bootstraps only ReleaseGuard topics and schemas"
}

resource "confluent_service_account" "runtime" {
  display_name = "releaseguard_runtime"
  description  = "Produces telemetry, consumes Flink results, and writes action audit events"
}

resource "confluent_service_account" "flink_app" {
  display_name = "releaseguard_flink_app"
  description  = "Execution principal for ReleaseGuard Flink statements"
}

resource "confluent_service_account" "flink_deployer" {
  display_name = "releaseguard_flink_deployer"
  description  = "Submits ReleaseGuard statements to the target Flink region"
}

resource "confluent_role_binding" "manager_environment_admin" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = data.confluent_environment.target.resource_name
}

resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.flink_deployer.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.target.resource_name
}

resource "confluent_role_binding" "flink_assigner" {
  principal   = "User:${confluent_service_account.flink_deployer.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.current.resource_name}/service-account=${confluent_service_account.flink_app.id}"
}

resource "confluent_role_binding" "flink_topic_read" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/topic=releaseguard_*"
}

resource "confluent_role_binding" "flink_topic_write" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/topic=releaseguard_*"
}

resource "confluent_role_binding" "flink_transaction_read" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/transactional-id=_confluent-flink_*"
}

resource "confluent_role_binding" "flink_transaction_write" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/transactional-id=_confluent-flink_*"
}

resource "confluent_role_binding" "flink_sr_read" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.target.resource_name}/subject=releaseguard_*"
}

resource "confluent_role_binding" "flink_sr_write" {
  principal   = "User:${confluent_service_account.flink_app.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.target.resource_name}/subject=releaseguard_*"
}

resource "confluent_role_binding" "runtime_sr_read" {
  principal   = "User:${confluent_service_account.runtime.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.target.resource_name}/subject=releaseguard_*"
}

resource "confluent_role_binding" "runtime_topic_read" {
  principal   = "User:${confluent_service_account.runtime.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/topic=releaseguard_*"
}

resource "confluent_role_binding" "runtime_topic_write" {
  principal   = "User:${confluent_service_account.runtime.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/topic=releaseguard_*"
}

resource "confluent_role_binding" "runtime_group_read" {
  principal   = "User:${confluent_service_account.runtime.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/group=releaseguard_*"
}

# Managed sink offsets use Confluent-owned connect-lcc-* group names. ResourceOwner
# supplies the READ, DESCRIBE, and DELETE permissions required for offset management.
resource "confluent_role_binding" "connector_group_owner" {
  count       = var.enable_http_sink ? 1 : 0
  principal   = "User:${confluent_service_account.runtime.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.target.rbac_crn}/kafka=${confluent_kafka_cluster.target.id}/group=connect-lcc-*"
}

resource "confluent_api_key" "manager_kafka" {
  display_name = "releaseguard_manager_kafka"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.target.id
    api_version = confluent_kafka_cluster.target.api_version
    kind        = confluent_kafka_cluster.target.kind
    environment { id = var.environment_id }
  }
  depends_on = [confluent_role_binding.manager_environment_admin]
}

resource "confluent_api_key" "manager_schema_registry" {
  display_name = "releaseguard_manager_schema_registry"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.target.id
    api_version = data.confluent_schema_registry_cluster.target.api_version
    kind        = data.confluent_schema_registry_cluster.target.kind
    environment { id = var.environment_id }
  }
  depends_on = [confluent_role_binding.manager_environment_admin]
}

resource "confluent_api_key" "runtime_kafka" {
  display_name = "releaseguard_runtime_kafka"
  owner {
    id          = confluent_service_account.runtime.id
    api_version = confluent_service_account.runtime.api_version
    kind        = confluent_service_account.runtime.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.target.id
    api_version = confluent_kafka_cluster.target.api_version
    kind        = confluent_kafka_cluster.target.kind
    environment { id = var.environment_id }
  }
  depends_on = [confluent_role_binding.runtime_topic_read, confluent_role_binding.runtime_topic_write, confluent_role_binding.runtime_group_read]
}

resource "confluent_api_key" "runtime_schema_registry" {
  display_name = "releaseguard_runtime_schema_registry"
  owner {
    id          = confluent_service_account.runtime.id
    api_version = confluent_service_account.runtime.api_version
    kind        = confluent_service_account.runtime.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.target.id
    api_version = data.confluent_schema_registry_cluster.target.api_version
    kind        = data.confluent_schema_registry_cluster.target.kind
    environment { id = var.environment_id }
  }
  depends_on = [confluent_role_binding.runtime_sr_read]
}

resource "confluent_api_key" "flink" {
  display_name = "releaseguard_flink"
  owner {
    id          = confluent_service_account.flink_deployer.id
    api_version = confluent_service_account.flink_deployer.api_version
    kind        = confluent_service_account.flink_deployer.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.target.id
    api_version = data.confluent_flink_region.target.api_version
    kind        = data.confluent_flink_region.target.kind
    environment { id = var.environment_id }
  }
  depends_on = [confluent_role_binding.flink_developer, confluent_role_binding.flink_assigner]
}

resource "confluent_kafka_topic" "releaseguard" {
  for_each = local.all_topics
  kafka_cluster { id = confluent_kafka_cluster.target.id }
  topic_name       = each.key
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.target.rest_endpoint
  config           = { "cleanup.policy" = "delete", "retention.ms" = each.value.retention }
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
}

resource "confluent_schema" "value" {
  for_each = local.value_schemas
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.target.id }
  rest_endpoint = data.confluent_schema_registry_cluster.target.rest_endpoint
  subject_name  = "${each.key}-value"
  format        = "AVRO"
  schema        = file("${path.module}/../schemas/${each.value}")
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
}

resource "confluent_schema" "decision_key" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.target.id }
  rest_endpoint = data.confluent_schema_registry_cluster.target.rest_endpoint
  subject_name  = "releaseguard_release_decisions-key"
  format        = "AVRO"
  schema        = file("${path.module}/../schemas/releaseguard_release_decisions_key.avsc")
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
}

resource "confluent_subject_config" "value" {
  for_each = local.value_schemas
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.target.id }
  rest_endpoint       = data.confluent_schema_registry_cluster.target.rest_endpoint
  subject_name        = "${each.key}-value"
  compatibility_level = "BACKWARD_TRANSITIVE"
  normalize           = true
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
  depends_on = [confluent_schema.value]
}

resource "confluent_subject_config" "decision_key" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.target.id }
  rest_endpoint       = data.confluent_schema_registry_cluster.target.rest_endpoint
  subject_name        = "releaseguard_release_decisions-key"
  compatibility_level = "BACKWARD_TRANSITIVE"
  normalize           = true
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
  depends_on = [confluent_schema.decision_key]
}

resource "confluent_flink_statement" "window_health" {
  count          = var.enable_flink ? 1 : 0
  statement_name = "releaseguard-window-health-v1"
  organization { id = data.confluent_organization.current.id }
  environment { id = var.environment_id }
  compute_pool { id = confluent_flink_compute_pool.target.id }
  principal { id = confluent_service_account.flink_app.id }
  statement = file("${path.module}/../flink/01_releaseguard_window_health.sql")
  properties = {
    "sql.current-catalog"  = data.confluent_environment.target.display_name
    "sql.current-database" = confluent_kafka_cluster.target.display_name
    "sql.local-time-zone"  = "UTC"
  }
  rest_endpoint = data.confluent_flink_region.target.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [
    confluent_kafka_topic.releaseguard,
    confluent_subject_config.value,
    confluent_role_binding.flink_topic_read,
    confluent_role_binding.flink_topic_write,
    confluent_role_binding.flink_transaction_read,
    confluent_role_binding.flink_transaction_write,
    confluent_role_binding.flink_sr_read,
    confluent_role_binding.flink_sr_write,
  ]
}

resource "confluent_flink_statement" "release_decisions" {
  count          = var.enable_flink ? 1 : 0
  statement_name = "releaseguard-release-decisions-v1"
  organization { id = data.confluent_organization.current.id }
  environment { id = var.environment_id }
  compute_pool { id = confluent_flink_compute_pool.target.id }
  principal { id = confluent_service_account.flink_app.id }
  statement = file("${path.module}/../flink/02_releaseguard_release_decisions.sql")
  properties = {
    "sql.current-catalog"  = data.confluent_environment.target.display_name
    "sql.current-database" = confluent_kafka_cluster.target.display_name
    "sql.local-time-zone"  = "UTC"
  }
  rest_endpoint = data.confluent_flink_region.target.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_flink_statement.window_health, confluent_subject_config.decision_key]
}

resource "confluent_connector" "http_sink" {
  count = var.enable_http_sink ? 1 : 0
  environment { id = var.environment_id }
  kafka_cluster { id = confluent_kafka_cluster.target.id }
  config_sensitive = { "bearer.token" = var.webhook_bearer_token }
  config_nonsensitive = {
    "connector.class"                               = "HttpSinkV2"
    "name"                                          = "releaseguard_http_sink_v2"
    "topics"                                        = "releaseguard_release_decisions"
    "api1.topics"                                   = "releaseguard_release_decisions"
    "apis.num"                                      = "1"
    "input.data.format"                             = "AVRO"
    "schema.context.name"                           = "default"
    "value.subject.name.strategy"                   = "TopicNameStrategy"
    "kafka.auth.mode"                               = "SERVICE_ACCOUNT"
    "kafka.service.account.id"                      = confluent_service_account.runtime.id
    "use.open.api.spec"                             = "false"
    "http.api.base.url"                             = trimsuffix(var.public_backend_base_url, "/")
    "auth.type"                                     = "BEARER"
    "https.host.verifier.enabled"                   = "true"
    "api1.http.api.path"                            = "/api/v1/release-decisions/{decisionId}?source=$${topic}"
    "api1.http.path.parameters"                     = "decisionId:$${decision_id}"
    "api1.http.request.method"                      = "POST"
    "api1.request.body.format"                      = "JSON"
    "api1.max.batch.size"                           = "1"
    "api1.batch.json.as.array"                      = "false"
    "api1.http.connect.timeout.ms"                  = "5000"
    "api1.http.request.timeout.ms"                  = "5000"
    "api1.max.retries"                              = "3"
    "api1.retry.backoff.ms"                         = "750"
    "api1.retry.backoff.policy"                     = "EXPONENTIAL_WITH_JITTER"
    "api1.retry.on.status.codes"                    = "408,429,500-"
    "auto.restart.on.user.error"                    = "true"
    "errors.tolerance"                              = "all"
    "behavior.on.error"                             = "IGNORE"
    "report.errors.as"                              = "http_response"
    "reporter.result.topic.name"                    = "releaseguard_http_success"
    "reporter.error.topic.name"                     = "releaseguard_http_error"
    "errors.deadletterqueue.topic.name"             = "releaseguard_http_dlq"
    "api1.report.only.status.code.to.success.topic" = "true"
    "consumer.override.auto.offset.reset"           = "latest"
    "consumer.override.isolation.level"             = "read_uncommitted"
    "tasks.max"                                     = "1"
  }
  lifecycle {
    precondition {
      condition     = startswith(var.public_backend_base_url, "https://") && length(var.webhook_bearer_token) >= 24
      error_message = "Enable HTTP Sink only with an HTTPS tunnel URL and a bearer token of at least 24 characters."
    }
    precondition {
      condition     = !var.enable_http_sink || var.enable_flink
      error_message = "HTTP Sink V2 requires the ReleaseGuard Flink statements."
    }
  }
  depends_on = [
    confluent_flink_statement.release_decisions,
    confluent_kafka_topic.releaseguard,
    confluent_role_binding.connector_group_owner,
  ]
}
