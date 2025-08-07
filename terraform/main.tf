resource "google_storage_bucket" "data_lake_bucket" {
  name          = "vibra-dtan-jur-anp-input-dev"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = false
  }
}