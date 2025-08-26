resource "google_cloud_run_v2_job" "extracao_codigos_instalacao_job" {
  name     = "cr-juridico-extracao-codigos-instalacao-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-codigos-instalacao:${var.image_version}"
        }
    }
  }
}