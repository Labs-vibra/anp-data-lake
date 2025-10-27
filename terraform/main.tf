resource "google_artifact_registry_repository" "anp_repo_etl" {
  repository_id = "ar-juridico-process-anp-datalake"
  location      = var.region
  format        = "DOCKER"

  description = "Repositório de artefatos para o projeto ANP"
  project     = local.project_id
}

module "logistica" {
  source              = "./modules/logistica"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "metas_individuais_cbios" {
  source              = "./modules/metas_individuais_cbios"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "aposentadoria_cbios" {
  source              = "./modules/aposentadoria-cbios"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "market_share" {
  source              = "./modules/market_share"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "codigos_instalacao" {
  source              = "./modules/codigos_instalacao"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "pmqc" {
  source              = "./modules/pmqc"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "contratos_cessao" {
  source              = "./modules/contratos_cessao"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "producao_biodiesel_m3" {
  source              = "./modules/producao_biodiesel_m3"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "postos_revendedores" {
  source              = "./modules/postos_revendedores"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "dados_fiscalizacao_do_abastecimento" {
  source              = "./modules/dados_fiscalizacao_do_abastecimento"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "vendas_comb_segmento" {
  source              = "./modules/vendas_comb_segmento"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "consulta_bases_de_distribuicao_e_trr_autorizados" {
  source              = "./modules/consulta_bases_de_distribuicao_e_trr_autorizados"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "multas_aplicadas_acoes_fiscalizacao" {
  source              = "./modules/multas_aplicadas_acoes_fiscalizacao"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}

module "tancagem_do_abastecimento_nacional_de_combustiveis" {
  source              = "./modules/tancagem_do_abastecimento_nacional_de_combustiveis"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}