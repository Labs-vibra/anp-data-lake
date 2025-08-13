resource "google_cloud_run_v2_job" "extracao_metas_cbios_2019_job" {
  name     = "extracao-metas-cbios-2019-job"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2019-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2022_job" {
  name     = "extracao-metas-cbios-2022-job"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2022-job:${var.image_version}"
        }
    }
  }
}