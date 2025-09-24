"""
Configuração centralizada das imagens Docker para o projeto ANP Data Lake
"""
import os
from dotenv import load_dotenv

load_dotenv()
# Configuration

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
ARTIFACT_REPO = os.getenv("ARTIFACT_REPO", "ar-juridico-process-anp-datalake")
ARTIFACT_REGISTRY_BASE_URL = f"us-central1-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/"

LOGISTICA_IMAGES = [
    {"label": "Extração de logística", "name": "run-extracao-logistica", "path": "./src/logistica/extracao"},
    {"label": "Extração de Logística 01", "name": "run-extracao-logistica-01", "path": "./src/logistica/logistica_01"},
    {"label": "Extração de Logística 02", "name": "run-extracao-logistica-02", "path": "./src/logistica/logistica_02"},
    {"label": "Extração de Logística 03", "name": "run-extracao-logistica-03", "path": "./src/logistica/logistica_03"},
]

CBIOS_METAS_IMAGES = [
    {"label": "Extração Metas CBIOs 2019", "name": "run-extracao-metas-cbios-2019-job", "path": "./src/metas_individuais_cbios/cbios_2019"},
    {"label": "Extração Metas CBIOs 2020", "name": "run-extracao-metas-cbios-2020-job", "path": "./src/metas_individuais_cbios/cbios_2020"},
    {"label": "Extração Metas CBIOs 2021", "name": "run-extracao-metas-cbios-2021-job", "path": "./src/metas_individuais_cbios/cbios_2021"},
    {"label": "Extração Metas CBIOs 2022", "name": "run-extracao-metas-cbios-2022-job", "path": "./src/metas_individuais_cbios/cbios_2022"},
    {"label": "Extração Metas CBIOs 2023", "name": "run-extracao-metas-cbios-2023-job", "path": "./src/metas_individuais_cbios/cbios_2023"},
    {"label": "Extração Metas CBIOs 2024", "name": "run-extracao-metas-cbios-2024-job", "path": "./src/metas_individuais_cbios/cbios_2024"},
    {"label": "Extração Metas CBIOs 2025", "name": "run-extracao-metas-cbios-2025-job", "path": "./src/metas_individuais_cbios/cbios_2025"},
]

CBIOS_RETIREMENT_IMAGES = [
    {"label": "Extração Aposentadoria CBIOs", "name": "run-extracao-aposentadoria-cbios-job", "path": "./src/aposentadoria_cbios"},
]

MARKET_SHARE_IMAGES = [
    {"label": "Extração Market Share", "name": "run-extracao-market-share-job", "path": "./src/market_share/extracao"},
    {"label": "Extração Market Share Distribuidor Atual", "name": "run-raw-distribuidor-atual", "path": "./src/market_share/raw_distribuidor_atual"},
    {"label": "Extração Liquidos Importacao Distribuidores", "name": "run-raw-importacao-distribuidores", "path": "./src/market_share/importacao_de_distribuidores"},
    {"label": "Extração Líquidos Histórico Entregas", "name": "run-raw-liquidos-historico-entregas", "path": "./src/market_share/historico_de_entregas"},
    {"label": "Extração Líquidos Vendas Atual", "name": "run-raw-vendas-atual", "path": "./src/market_share/liquidos_vendas_atual"},
    {"label": "Extração Líquidos Entregas Fornecedor Atual", "name": "run-raw-entregas-fornecedor-atual", "path": "./src/market_share/liquidos_entregas_fornecedor_atual"},
    {"label": "Extração Líquidos Histórico Vendas", "name": "run-raw-liquidos-historico-vendas", "path": "./src/market_share/historico_de_vendas"}
]

INSTALLATION_CODES_IMAGES = [
    {"label": "Extração Códigos de Instalação", "name": "run-extracao-codigos-instalacao", "path": "./src/manual_simp/codigos_instalacao"},
    {"label": "Extração Manual SIMP", "name": "run-extracao-manual-simp", "path": "./src/manual_simp/extracao"},
]

DISTRIBUIDORES_COMB_LIQ_IMAGES = [
    {"label": "Extração Distribuidores Combustíveis Líquidos", "name": "run-distribuidores-comb-liq-auto-exercicio-ativ", "path": "./src/distribuidores_comb_liq_auto_exercicio_ativ"},
]

PMQC_IMAGES = [
    {"label": "Extração Programa de Monitoramento da Qualidade dos Combustíveis (PMQC)", "name": "run-raw-pmqc-job", "path": "./src/pmqc"},
]

TANCAGEM_DO_ABASTECIMENTO_NACIONAL_DE_COMBUSTIVEIS_IMAGES = [
    {"label": "Extração Tancagem do Abastecimento Nacional de Combustíveis", "name": "run-raw-tancagem-do-abastecimento-nacional-de-combustiveis-job", "path": "./src/tancagem_do_abastecimento_nacional_de_combustiveis" },
]

PRODUCAO_BIODIESEL_M3 = [
    {"label": "Extração Produção de biodiesel m3", "name": "run-raw-producao-biodiesel-m3-job", "path": "./src/producao_biodiesel_m3"},    
]

DOCKER_IMAGES = [
    # *LOGISTICA_IMAGES,
    # *CBIOS_METAS_IMAGES,
    *CBIOS_RETIREMENT_IMAGES,
    *MARKET_SHARE_IMAGES,
    *INSTALLATION_CODES_IMAGES,
    *DISTRIBUIDORES_COMB_LIQ_IMAGES,
    CBIOS_METAS_IMAGES[1],
    *PMQC_IMAGES,
    *TANCAGEM_DO_ABASTECIMENTO_NACIONAL_DE_COMBUSTIVEIS_IMAGES,
    *PRODUCAO_BIODIESEL_M3
]

def get_image_by_name(image_name):
    """
    Busca uma imagem pelo nome

    Args:
        image_name (str): Nome da imagem Docker

    Returns:
        dict or None: Configuração da imagem se encontrada, None caso contrário
    """
    for image in DOCKER_IMAGES:
        if image["name"] == image_name:
            return image
    return None

def get_all_images():
    """
    Retorna todas as imagens disponíveis

    Returns:
        list: Lista com todas as configurações de imagens Docker
    """
    return DOCKER_IMAGES.copy()

def get_images_by_module(module_name):
    """
    Busca imagens por módulo baseado no path

    Args:
        module_name (str): Nome do módulo (ex: 'logistica', 'metas_individuais_cbios')

    Returns:
        list: Lista com as imagens do módulo especificado
    """
    return [img for img in DOCKER_IMAGES if module_name in img["path"]]