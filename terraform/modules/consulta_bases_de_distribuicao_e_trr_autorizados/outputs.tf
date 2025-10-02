output "extracao_consulta_bases_de_distribuicao_e_trr_autorizados_job_name" {
  description = "Nome do job de extração Consulta Bases de Distribuição e TRR Autorizados"
  value       = google_cloud_run_v2_job.extracao_consulta_bases_de_distribuicao_e_trr_autorizados.name
}

output "raw_consulta_bases_de_distribuicao_e_trr_autorizados_job_name" {
  description = "Nome do job de extração Consulta Bases de Distribuição e TRR Autorizados"
  value       = google_cloud_run_v2_job.raw__consulta_bases_de_distribuicao_e_trr_autorizados.name
}
