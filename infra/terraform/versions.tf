terraform {
  required_version = ">= 1.6, < 2.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "= 2.83.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}
