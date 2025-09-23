variable "is_prod" {
  description = "Whether this is a production environment"
  type        = bool
  default     = false
}

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
  description = "Service account email"
  type        = string
  default     = "reginafaria@vibraenergia.com.br"
}

variable "jobs_image_base_url" {
  type        = string
  default     = "us-central1-docker.pkg.dev/ext-ecole-biomassa/ar-juridico-process-anp-datalake"
  description = "Base URL for the Docker image"
}

variable "image_version" {
  type        = string
  default     = "latest"
  description = "Version of the Docker image"
}

locals {
  is_prod = var.is_prod

  project_id          = "ext-ecole-biomassa"
  bucket_name         = "vibra-dtan-jur-anp-input"
  service_account     = "reginafaria@vibraenergia.com.br"
  jobs_image_base_url = "us-central1-docker.pkg.dev/${local.project_id}/ar-juridico-process-anp-datalake"
}