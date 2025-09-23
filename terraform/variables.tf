variable "is_prod" {
  description = "Whether this is a production environment"
  type        = bool
  default     = false
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "ext-ecole-biomassa"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "vibra-dtan-jur-anp-input"
}

variable "service_account" {
  description = "Service account email"
  type        = string
  default     = "reginafaria@vibraenergia.com.br"
  # default     = "gcp-agent@ext-ecole-biomassa-468317.iam.gserviceaccount.com"
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

  # Computed values based on environment
  project_id          = var.is_prod ? "ext-ecole-biomassa" : "ext-ecole-biomassa-468317"
  bucket_name         = var.is_prod ? "vibra-dtan-jur-anp-input" : "ext-ecole-biomassa"
  service_account     = var.is_prod ? "reginafaria@vibraenergia.com.br" : "gcp-agent@ext-ecole-biomassa-468317.iam.gserviceaccount.com"
  jobs_image_base_url = "us-central1-docker.pkg.dev/${local.project_id}/ar-juridico-process-anp-datalake"
}