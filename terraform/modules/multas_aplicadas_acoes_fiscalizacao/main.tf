resource "google_cloud_run_v2_job" "extracao_multas_aplicadas_acoes_fiscalizacao" {
  name     = "cr-juridico-multas-aplicadas-acoes-fiscalizacao-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/cr-juridico-raw-multas-aplicadas-acoes-fiscalizacao-job:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "raw_multas_aplicadas_acoes_fiscalizacao" {
  name     = "cr-juridico-rw-multas-aplicadas-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/cr-juridico-raw-multas-aplicadas-job:${var.image_version}"
      }
    }
  }
}
