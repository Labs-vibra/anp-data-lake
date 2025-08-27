resource "google_cloud_run_v2_job" "extracao_manual_simp_job" {
  name     = "cr-juridico-extracao-manual-simp-job-dev"
  location = var.region

  template {
    template {
        containers {
            image = "${var.jobs_image_base_url}/run-extracao-manual-simp:${var.image_version}"
        }
    }
  }
}