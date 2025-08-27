resource "google_cloud_run_v2_job" "run-extracao-market-share" {
  name     = "cr-juridico-extracao-market-share-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-market-share:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "run_raw_distribuidor_atual" {
  name     = "cr-juridico-raw-distribuidor-atual-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-raw-distribuidor-atual:${var.image_version}"
        }
    }
  }
}