resource "google_cloud_run_v2_job" "raw_tancagem_do_abastecimento_nacional_de_combustiveis_job" {
  name     = "cr-juridico-rw-tancagem-abastecimento-nacional-de-comb-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-tancagem-do-abastecimento-nacional-de-combustiveis-job:${var.image_version}"
      }
    }
  }
}
