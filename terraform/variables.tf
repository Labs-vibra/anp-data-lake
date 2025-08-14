variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "ext-ecole-biomassa-468317"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "ext-ecole-biomassa"
}

variable "service_account" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "gcp-agent@ext-ecole-biomassa-468317.iam.gserviceaccount.com"
}

variable jobs_image_base_url {
  type        = string
  default     = "us-central1-docker.pkg.dev/ext-ecole-biomassa-468317/ar-juridico-process-anp-datalake"
  description = "Base URL for the Docker image"
}

variable "image_version" {
  type        = string
  default     = "latest"
  description = "Version of the Docker image"
}