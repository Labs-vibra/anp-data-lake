resource "google_cloud_run_v2_job" "raw_dados_fiscalizacao_do_abastecimento_job" {
  name     = "cr-juridico-rw-dados-fiscalizacao-do-abastecimento-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-dados-fiscalizacao-do-abastecimento-job:${var.image_version}"
      }
    }
  }
}
