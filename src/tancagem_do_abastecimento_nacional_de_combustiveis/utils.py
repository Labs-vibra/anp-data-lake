import re
import unicodedata
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from google.cloud import storage
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_column(col: str) -> str:
    """
    Normaliza nomes de colunas:
    - Remove acentos e caracteres estranhos
    - Converte para lowercase
    - Substitui espaços e caracteres inválidos por underscore
    """
    col = col.strip().lower()
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')
    col = re.sub(r'[^a-z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

def fetch_html(url: str):
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")

def find_all_csv_links(soup):
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".csv") or href.endswith(".xlsx"):
            links.append(a["href"])
    return links

def download_file(url: str) -> BytesIO:
    logger.info(f"Baixando arquivo: {url}")
    response = requests.get(url, timeout=300, verify=False)
    response.raise_for_status()
    return BytesIO(response.content)

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    logger.info(f"Arquivo enviado para bucket: {nome_no_bucket}")
    return True
