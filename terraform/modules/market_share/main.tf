resource "google_cloud_run_service" "market_share_extraction" {
  name     = "cr-juridico-extracao-market-share-job-dev"
  location = var.region

  template {
    spec {
      containers {
        image = "${local.jobs_image_base_url}/run-extracao-market-share:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_service" "raw_distribuidor_atual" {
  name     = "cr-juridico-raw-distribuidor-atual-job-dev"
  location = var.region

  template {
    spec {
      containers {
        image = "${local.jobs_image_base_url}/run-raw-distribuidor-atual:${var.image_version}"
      }
    }
  }
}


resource "google_cloud_run_service" "raw_importacao_distribuidores" {
  name     = "cr-juridico-raw-importacao-distribuidores-job-dev"
  location = var.region

  template {
    spec {
      containers {
        image = "${local.jobs_image_base_url}/run-raw-importacao-distribuidores:${var.image_version}"
      }
    }
  }
}