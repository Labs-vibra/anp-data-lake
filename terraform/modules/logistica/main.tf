resource "google_cloud_run_v2_job" "extracao_logistica_job" {
  name     = "cr-juridico-extracao-logistica-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-logistica:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_logistica_01_job" {
  name     = "cr-juridico-extracao-logistica-01-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-logistica-01:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_logistica_02_job" {
  name     = "cr-juridico-extracao-logistica-02-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-logistica-02:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_logistica_03_job" {
  name     = "cr-juridico-extracao-logistica-03-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-logistica-03:${var.image_version}"
      }
    }
  }
}
