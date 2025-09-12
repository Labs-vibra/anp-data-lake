import datetime
import os
import re
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from google.cloud import storage
from venv import logging
from constants import BUCKET_NAME

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def normalize_filename(filename: str, year: str) -> str:
    """
    Normaliza nomes de arquivos PMQC para o padrão pmqc_YYYY_MM.csv.
    """
    match = re.search(r'(\d{4})[_-]?(\d{2})', filename)
    if match:
        year_match, month = match.groups()
        return f"pmqc_{year_match}_{month}.csv"

    match_month_only = re.search(r'(\d{2})', filename)
    if match_month_only:
        month = match_month_only.group(1)
        return f"pmqc_{year}_{month}.csv"

    return filename

def fetch_html(url):
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")

def find_all_csv_links(soup):
    """
    Busca todos os links CSV dentro do conteúdo principal da página de PMQC.
    Retorna lista de URLs.
    """
    csv_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.endswith(".csv"):
            csv_links.append(href)
    return csv_links

def download_file(url):
    """
    Faz download de arquivo CSV e retorna em BytesIO.
    """
    logger.info(f"Baixando arquivo: {url}")
    response = requests.get(url, timeout=300, verify=False)
    response.raise_for_status()
    return BytesIO(response.content)

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    print(f"Arquivo {arquivo_bytes} enviado como {nome_no_bucket}.")
    return True
