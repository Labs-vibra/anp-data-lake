resource "google_cloud_run_v2_job" "raw_producao_biodiesel_m3_job" {
  name     = "cr-juridico-rw-producao-biodiesel-m3-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-producao-biodiesel-m3-job:${var.image_version}"
      }
    }
  }
}
