output "runtime" {
  sensitive = true
  value = {
    kafka_bootstrap_servers    = confluent_kafka_cluster.target.bootstrap_endpoint
    kafka_api_key              = confluent_api_key.runtime_kafka.id
    kafka_api_secret           = confluent_api_key.runtime_kafka.secret
    schema_registry_url        = data.confluent_schema_registry_cluster.target.rest_endpoint
    schema_registry_api_key    = confluent_api_key.runtime_schema_registry.id
    schema_registry_api_secret = confluent_api_key.runtime_schema_registry.secret
  }
}

output "resource_summary" {
  value = {
    environment      = data.confluent_environment.target.display_name
    environment_id   = data.confluent_environment.target.id
    kafka_cluster    = confluent_kafka_cluster.target.display_name
    kafka_cluster_id = confluent_kafka_cluster.target.id
    flink_pool_id    = confluent_flink_compute_pool.target.id
    topics           = sort(keys(local.all_topics))
    flink_statements = var.enable_flink ? ["releaseguard-window-health-v1", "releaseguard-release-decisions-v1"] : []
    connector        = var.enable_http_sink ? "releaseguard_http_sink_v2" : null
  }
}
