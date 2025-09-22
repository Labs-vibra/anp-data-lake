resource "google_artifact_registry_repository" "anp_repo_etl" {
  repository_id = "ar-juridico-process-anp-datalake"
  location      = var.region
  format        = "DOCKER"

  description = "Repositório de artefatos para o projeto ANP"
  project     = local.project_id
}

# resource "google_composer_environment" "anp_composer" {
#   name    = "composer-jur-anp-dev"
#   region  = var.region
#   project = local.project_id

#   config {
#     node_config {
#       service_account = local.service_account
#     }
#     software_config {
#       image_version = "composer-3-airflow-2.10.5-build.9"
#       pypi_packages = {
#         "apache-airflow-providers-google" = ""
#       }
#     }
#     environment_size = "ENVIRONMENT_SIZE_SMALL"
#   }
# }

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