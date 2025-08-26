resource "google_cloud_run_v2_job" "run-extracao-market-share" {
  name     = "cr-juridico-extracao-market-share-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-market-share:${var.image_version}"
        }
    }
  }
}