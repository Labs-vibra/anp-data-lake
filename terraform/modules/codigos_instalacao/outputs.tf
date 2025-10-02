output "manual_simp_job_name" {
  description = "Nome do job de extração manual SIMP"
  value       = google_cloud_run_v2_job.extracao_manual_simp_job.name
}

output "codigos_instalacao_job_name" {
  description = "Nome do job de extração de códigos de instalação"
  value       = google_cloud_run_v2_job.extracao_codigos_instalacao_job.name
}