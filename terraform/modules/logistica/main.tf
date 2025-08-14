resource "google_cloud_run_v2_job" "extracao_logistica_job" {
  name     = "extracao-logistica-job"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-logistica:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_logistica_job" {
  name     = "extracao-logistica-01-job"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-logistica-01:${var.image_version}"
        }
    }
  }
}
