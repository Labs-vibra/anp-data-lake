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

DOCKER_IMAGES = [
    # Módulo logística
    {
        "label": "Extração de logística",
        "name": "run-extracao-logistica",
        "path": "./src/logistica/extracao",
    },
    {
        "label": "Extração de Logística 01",
        "name": "run-extracao-logistica-01",
        "path": "./src/logistica/logistica_01",
    },
    {
        "label": "Extração de Logística 02",
        "name": "run-extracao-logistica-02",
        "path": "./src/logistica/logistica_02",
    },
    {
        "label": "Extração de Logística 03",
        "name": "run-extracao-logistica-03",
        "path": "./src/logistica/logistica_03",
    },
    # Módulo metas individuais CBIOs
    {
        "label": "Extração Metas CBIOs 2019",
        "name": "run-extracao-metas-cbios-2019-job",
        "path": "./src/metas_individuais_cbios/cbios_2019",
    },
    {
        "label": "Extração Metas CBIOs 2020",
        "name": "run-extracao-metas-cbios-2020-job",
        "path": "./src/metas_individuais_cbios/cbios_2020",
    },
    {
        "label": "Extração Metas CBIOs 2021",
        "name": "run-extracao-metas-cbios-2021-job",
        "path": "./src/metas_individuais_cbios/cbios_2021",
    },
    {
        "label": "Extração Metas CBIOs 2022",
        "name": "run-extracao-metas-cbios-2022-job",
        "path": "./src/metas_individuais_cbios/cbios_2022",
    },
    {
        "label": "Extração Metas CBIOs 2023",
        "name": "run-extracao-metas-cbios-2023-job",
        "path": "./src/metas_individuais_cbios/cbios_2023",
    },
    {
        "label": "Extração Metas CBIOs 2024",
        "name": "run-extracao-metas-cbios-2024-job",
        "path": "./src/metas_individuais_cbios/cbios_2024",
    },
    {
        "label": "Extração Metas CBIOs 2025",
        "name": "run-extracao-metas-cbios-2025-job",
        "path": "./src/metas_individuais_cbios/cbios_2025",
    }
    # Módulo Market Share
    ,{
        "label": "Extração Market Share",
        "name": "run-extracao-market-share-job",
        "path": "./src/market_share/extracao",
    },
    {
        "label": "Extração Market Share Distribuidor Atual",
        "name": "run-raw-distribuidor-atual",
        "path": "./src/market_share/raw_distribuidor_atual",
    }
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
