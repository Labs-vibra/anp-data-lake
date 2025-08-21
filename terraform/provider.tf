terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "ext-ecole-biomassa" #  var.is_prod ? "vibra-dtan-jur-anp-input" : "ext-ecole-biomassa"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = local.project_id
  region  = var.region
}
