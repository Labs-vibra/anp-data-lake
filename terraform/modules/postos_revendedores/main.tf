resource "google_cloud_run_v2_job" "raw_postos_revendedores_job" {
  name     = "cr-juridico-rw-postos-revendedores-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-postos-revendedores-job:${var.image_version}"
      }
    }
  }
}
