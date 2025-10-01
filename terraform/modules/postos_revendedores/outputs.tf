output "postos_revendedores_job_name" {
  description = "Nome do job de Postos Revendedores"
  value       = google_cloud_run_v2_job.extracao_postos_revendedores_job.name
}
