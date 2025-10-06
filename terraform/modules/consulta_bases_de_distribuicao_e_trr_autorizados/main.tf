resource "google_cloud_run_v2_job" "extracao_consulta_bases_de_distribuicao_e_trr_autorizados" {
  name     = "cr-juridico-extracao-consulta-bases-de-distribuicao-e-trr-autorizados-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run_extracao_consulta_bases_de_distribuicao_e_trr_autorizados:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "raw_consulta_bases_de_distribuicao_e_trr_autorizados" {
  name     = "cr-juridico-rw-consulta-bases-de-distribuicao-e-trr-autorizados-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-rw-consulta-bases-de-distribuicao-e-trr-autorizados:${var.image_version}"
      }
    }
  }
}
