output "runtime" {
  sensitive = true
  value = {
    kafka_bootstrap_servers    = data.confluent_kafka_cluster.target.bootstrap_endpoint
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
    kafka_cluster    = data.confluent_kafka_cluster.target.display_name
    topics           = sort(keys(local.all_topics))
    flink_statements = var.enable_flink ? ["releaseguard_window_health_v1", "releaseguard_release_decisions_v1"] : []
    connector        = var.enable_http_sink ? "releaseguard_http_sink_v2" : null
  }
}
