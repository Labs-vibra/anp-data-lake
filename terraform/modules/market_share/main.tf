resource "google_cloud_run_v2_job" "market-share-extraction" {
  name     = "cr-juridico-extracao-market-share-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-market-share-job:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "raw-distribuidor-atual" {
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

resource "google_cloud_run_v2_job" "raw-importacao-distribuidores" {
  name     = "cr-juridico-raw-importacao-distribuidores-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-importacao-distribuidores:${var.image_version}"
      }
    }
  }
}


resource "google_cloud_run_v2_job" "run-raw-vendas-atual" {
  name     = "cr-juridico-raw-vendas-atual-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-vendas-atual:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "run-raw-liquidos-entrega-historico" {
  name     = "cr-juridico-raw-liquidos-entrega-historico-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-liquidos-historico-entregas:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "run-raw-entregas-fornecedor-atual" {
  name     = "cr-juridico-raw-entregas-fornecedor-atual-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-entregas-fornecedor-atual:${var.image_version}"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "run-raw-historico_de_entregas" {
  name     = "cr-juridico-raw-liquidos-entrega-historico-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-liquidos-entrega-historico:${var.image_version}"
      }
    }
  }
}

