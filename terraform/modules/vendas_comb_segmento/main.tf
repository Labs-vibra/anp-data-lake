resource "google_cloud_run_v2_job" "extracao_vendas-comb-segmento_job" {
  name     = "cr-juridico-extracao-vendas-comb-segmento-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-extracao-vendas-comb-segmento-job:${var.image_version}"
      }
    }
  }
}