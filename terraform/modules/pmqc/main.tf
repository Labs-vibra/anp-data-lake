resource "google_cloud_run_v2_job" "raw_pmqc_job" {
  name     = "cr-juridico-rw-pmqc-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-pmqc-job:${var.image_version}"
      }
    }
  }
}
