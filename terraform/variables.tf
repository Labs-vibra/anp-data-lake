variable "project" {
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

