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
