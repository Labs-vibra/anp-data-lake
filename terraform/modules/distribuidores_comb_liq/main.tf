resource "google_cloud_run_v2_job" "distribuidores_comb_liq_job" {
  name     = "cr-juridico-distribuidores-comb-liq-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-distribuidores-comb-liq-auto-exercicio-ativ:${var.image_version}"
    }
  }
}
