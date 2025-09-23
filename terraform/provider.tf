terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "vibra-dtan-jur-anp-input"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = local.project_id
  region  = var.region
}
