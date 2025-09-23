resource "google_cloud_run_v2_job" "distribuidores_comb_liq_job" {
  name     = "cr-juridico-distribuidores-comb-liq-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-distribuidores-comb-liq-auto-exercicio-ativ:${var.image_version}"

        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
      }

      task_timeout = "3600s"
      max_retries  = 3
    }
  }
}
