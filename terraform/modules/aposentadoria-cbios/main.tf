resource "google_cloud_run_v2_job" "extracao_aposentadoria_cbios_job" {
  name     = "cr-juridico-extracao-aposentadoria-cbios-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-aposentadoria-cbios-job:${var.image_version}"
        }
    }
  }
}