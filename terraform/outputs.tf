# Artifact Registry Repository
output "artifact_registry_repository_url" {
  description = "URL do repositório do Artifact Registry"
  value       = google_artifact_registry_repository.anp_repo_etl.name
}

# Market Share Jobs
output "market_share_jobs" {
  description = "Lista de jobs do Cloud Run para Market Share"
  value = {
    extraction       = module.market_share.market_share_extraction_job_name
    raw_distribuidor = module.market_share.raw_distribuidor_atual_job_name
    raw_importacao   = module.market_share.raw_importacao_distribuidores_job_name
    raw_vendas       = module.market_share.raw_vendas_atual_job_name
    raw_entregas     = module.market_share.raw_entregas_fornecedor_atual_job_name
    raw_liquidos     = module.market_share.raw_liquidos_entrega_historico_job_name
  }
}

# Aposentadoria CBIOs Jobs
output "aposentadoria_cbios_jobs" {
  description = "Lista de jobs do Cloud Run para Aposentadoria CBIOs"
  value = {
    extraction = module.aposentadoria_cbios.aposentadoria_cbios_job_name
  }
}

# Logística Jobs
output "logistica_jobs" {
  description = "Lista de jobs do Cloud Run para Logística"
  value = {
    extraction   = module.logistica.logistica_extraction_job_name
    logistica_01 = module.logistica.logistica_01_job_name
    logistica_02 = module.logistica.logistica_02_job_name
    logistica_03 = module.logistica.logistica_03_job_name
  }
}

# Metas Individuais CBIOs Jobs
output "metas_individuais_cbios_jobs" {
  description = "Lista de jobs do Cloud Run para Metas Individuais CBIOs"
  value = {
    cbios_2019 = module.metas_individuais_cbios.metas_cbios_2019_job_name
    cbios_2020 = module.metas_individuais_cbios.metas_cbios_2020_job_name
    cbios_2021 = module.metas_individuais_cbios.metas_cbios_2021_job_name
    cbios_2022 = module.metas_individuais_cbios.metas_cbios_2022_job_name
    cbios_2023 = module.metas_individuais_cbios.metas_cbios_2023_job_name
    cbios_2024 = module.metas_individuais_cbios.metas_cbios_2024_job_name
    cbios_2025 = module.metas_individuais_cbios.metas_cbios_2025_job_name
  }
}

# Códigos Instalação Jobs
output "codigos_instalacao_jobs" {
  description = "Lista de jobs do Cloud Run para Códigos de Instalação"
  value = {
    manual_simp        = module.codigos_instalacao.manual_simp_job_name
    codigos_instalacao = module.codigos_instalacao.codigos_instalacao_job_name
  }
}

# Contratos cessão Jobs
output "contratos_cessao_jobs" {
  description = "Lista de jobs do Cloud Run para Contratos cessão"
  value = {
    extraction = module.contratos_cessao.contratos_cessao_job_name
  }
}

# Project Information
output "project_id" {
  description = "ID do projeto GCP"
  value       = local.project_id
}

output "region" {
  description = "Região configurada"
  value       = var.region
}