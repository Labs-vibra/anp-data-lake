output "dados_fiscalizacao_do_abastecimento_job_name" {
  description = "Nome do job de Dados Fiscalização do Abastecimento"
  value       = google_cloud_run_v2_job.raw_dados_fiscalizacao_do_abastecimento_job.name
}
