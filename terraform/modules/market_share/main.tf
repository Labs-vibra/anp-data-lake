resource "google_cloud_run_service" "market_share_extraction" {
  name     = "cr-juridico-extracao-market-share-job-dev"
  location = var.region

  template {
    spec {
      containers {
        image = "${local.jobs_image_base_url}/run-extracao-market-share:${var.image_version}"
      }
    }
  }
}
