<<<<<<< HEAD
resource "google_cloud_run_v2_job" "raw_producao_biodiesel_m3_regiao_job" {
  name     = "cr-juridico-rw-producao-biodiesel-m3-regiao-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-producao-biodiesel-m3-regiao-job:${var.image_version}"
      }
    }
  }
}
=======
resource "google_cloud_run_v2_job" "raw_producao_biodiesel_m3_geral_job" {
  name     = "cr-juridico-rw-producao-biodiesel-m3-geral-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-producao-biodiesel-m3-geral-job:${var.image_version}"
      }
    }
  }
}
>>>>>>> develop
