output "contratos_cessao_job_name" {
  description = "Nome do job contratos cessão"
  value       = google_cloud_run_v2_job.extracao_contratos_cessao_job.name
}
