output "pmqc_job_name" {
  description = "Nome do job de PMQC"
  value       = google_cloud_run_v2_job.extracao_pmqc_job.name
}
