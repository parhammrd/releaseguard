variable "confluent_cloud_api_key" {
  description = "Bootstrap-only Confluent Cloud Global API key. Pass with TF_VAR; never commit it."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Bootstrap-only Confluent Cloud Global API secret. Pass with TF_VAR; never commit it."
  type        = string
  sensitive   = true
}

variable "environment_id" {
  description = "Existing Confluent environment that will contain only ReleaseGuard resources."
  type        = string
  default     = "env-g2k97n"
}

variable "cloud" {
  description = "Cloud provider for the ReleaseGuard Kafka cluster and Flink pool."
  type        = string
  default     = "AWS"
}

variable "region" {
  description = "Cloud region for the ReleaseGuard Kafka cluster and Flink pool."
  type        = string
  default     = "us-east-2"
}

variable "flink_max_cfu" {
  description = "Maximum autoscaling capacity for the two ReleaseGuard Flink statements."
  type        = number
  default     = 5

  validation {
    condition     = contains([5, 10, 20, 30, 40, 50], var.flink_max_cfu)
    error_message = "flink_max_cfu must be one of 5, 10, 20, 30, 40, or 50."
  }
}

variable "enable_flink" {
  description = "Create the two continuously running ReleaseGuard Flink statements."
  type        = bool
  default     = true
}

variable "enable_http_sink" {
  description = "Create HTTP Sink V2 after the disposable HTTPS tunnel is available."
  type        = bool
  default     = false
}

variable "public_backend_base_url" {
  description = "Disposable Cloudflare Quick Tunnel HTTPS origin, without a trailing slash."
  type        = string
  default     = ""
}

variable "webhook_bearer_token" {
  description = "Bearer token shared only by HTTP Sink V2 and the ReleaseGuard webhook."
  type        = string
  sensitive   = true
  default     = ""
}
