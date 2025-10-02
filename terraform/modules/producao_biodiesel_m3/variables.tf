variable "region" {
  description = "GCP region"
  type        = string
}

variable "jobs_image_base_url" {
  description = "Base URL for the Docker image repository"
  type        = string
}

variable "image_version" {
  description = "Version of the Docker image"
  type        = string
}
