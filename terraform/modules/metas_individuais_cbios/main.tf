resource "google_cloud_run_v2_job" "extracao_metas_cbios_2019_job" {
  name     = "cr-juridico-extracao-metas-cbios-2019-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2019-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2020_job" {
  name     = "cr-juridico-extracao-metas-cbios-2020-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2020-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2021_job" {
  name     = "cr-juridico-extracao-metas-cbios-2021-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2021-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2022_job" {
  name     = "cr-juridico-extracao-metas-cbios-2022-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2022-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2023_job" {
  name     = "cr-juridico-extracao-metas-cbios-2023-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2023-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2024_job" {
  name     = "cr-juridico-extracao-metas-cbios-2024-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2024-job:${var.image_version}"
        }
    }
  }
}

resource "google_cloud_run_v2_job" "extracao_metas_cbios_2025_job" {
  name     = "cr-juridico-extracao-metas-cbios-2025-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-metas-cbios-2025-job:${var.image_version}"
        }
    }
  }
}