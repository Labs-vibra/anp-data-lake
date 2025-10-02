output "logistica_extraction_job_name" {
  description = "Nome do job de extração da logística"
  value       = google_cloud_run_v2_job.extracao_logistica_job.name
}

output "logistica_01_job_name" {
  description = "Nome do job logística 01"
  value       = google_cloud_run_v2_job.extracao_logistica_01_job.name
}

output "logistica_02_job_name" {
  description = "Nome do job logística 02"
  value       = google_cloud_run_v2_job.extracao_logistica_02_job.name
}

output "logistica_03_job_name" {
  description = "Nome do job logística 03"
  value       = google_cloud_run_v2_job.extracao_logistica_03_job.name
}