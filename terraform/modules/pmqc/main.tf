resource "google_cloud_run_v2_job" "extracao_pmqc_job" {
  name     = "cr-juridico-extracao-pmqc-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-pmqc-job:${var.image_version}"
      }
    }
  }
}
