resource "google_cloud_run_v2_job" "extracao_bases_de_distribuicao_e_trr_autorizados" {
  name     = "cr-juridico-extracao-bases-distribuicao-trr-autorizados-job"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-consulta-bases-de-distribuicao-e-trr-autorizados-job:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "raw_bases_de_distribuicao_e_trr_autorizados" {
  name     = "cr-juridico-rw-trr-autorizados-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-consulta-bases-de-distribuicao-e-trr-autorizados-job:${var.image_version}"
      }
    }
  }
}
