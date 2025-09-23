output "producao_biodiesel_m3_job_name" {
  description = "Nome do job de Produção de biodiesel m3"
  value       = google_cloud_run_v2_job.extracao_producao_biodiesel_m3_job.name
}
