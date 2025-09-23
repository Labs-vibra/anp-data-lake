resource "google_cloud_run_v2_job" "extracao_contratos_cessao_job" {
  name     = "cr-juridico-extracao-contratos-cessao-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-contratos-cessao-job:${var.image_version}"
      }
    }
  }
}