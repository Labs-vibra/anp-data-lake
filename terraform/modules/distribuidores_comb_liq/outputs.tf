output "distribuidores_comb_liq_job_name" {
  description = "Nome do job de distribuidores de combustíveis líquidos"
  value       = google_cloud_run_v2_job.distribuidores_comb_liq_job.name
}
