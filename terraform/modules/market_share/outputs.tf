output "market_share_extraction_job_name" {
  description = "Nome do job de extração do Market Share"
  value       = google_cloud_run_v2_job.market-share-extraction.name
}

output "raw_distribuidor_atual_job_name" {
  description = "Nome do job de distribuidor atual"
  value       = google_cloud_run_v2_job.raw-distribuidor-atual.name
}

output "raw_importacao_distribuidores_job_name" {
  description = "Nome do job de importação de distribuidores"
  value       = google_cloud_run_v2_job.raw-importacao-distribuidores.name
}

output "raw_vendas_atual_job_name" {
  description = "Nome do job de vendas atual"
  value       = google_cloud_run_v2_job.run-raw-vendas-atual.name
}

output "raw_liquidos_entrega_historico_job_name" {
  description = "Nome do job de líquidos entrega histórico"
  value       = google_cloud_run_v2_job.run-raw-liquidos-entrega-historico.name
}

output "raw_entregas_fornecedor_atual_job_name" {
  description = "Nome do job de entregas fornecedor atual"
  value       = google_cloud_run_v2_job.run-raw-entregas-fornecedor-atual.name
}