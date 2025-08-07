terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }

  backend "gcs" {
    bucket = "ext-ecole-biomassa" // Replace with the actual bucket name
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project
  region  = var.region
}
