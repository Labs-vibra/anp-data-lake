output "tancagem_do_abastecimento_nacional_de_combustiveis_job_name" {
  description = "Nome do job de Tancagem do Abastecimento Nacional de Combustíveis"
  value       = google_cloud_run_v2_job.raw_tancagem_do_abastecimento_nacional_de_combustiveis_job.name
}
