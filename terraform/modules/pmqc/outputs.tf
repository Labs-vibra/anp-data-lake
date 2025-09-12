output "pmqc_job_name" {
  description = "Nome do job de aposentadoria CBIOs"
  value       = google_cloud_run_v2_job.extracao_pmqc_job.name
}
