output "vendas_comb_segmento_job_name" {
  description = "Nome do job vendas de combustíveis por segmento"
  value       = google_cloud_run_v2_job.extracao_vendas_comb_segmento_job.name
}
