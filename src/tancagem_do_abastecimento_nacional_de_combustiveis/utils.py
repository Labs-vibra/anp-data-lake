import re
import unicodedata
import pandas as pd
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from google.cloud import storage
import logging
from constants import URL_BASE, COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def camel_to_snake(name: str) -> str:
    """Converte CamelCase em snake_case e remove BOM e espaços extras."""
    name = str(name).strip().encode('utf-8').decode('utf-8-sig')
    name = name[0].lower() + name[1:] if name else name
    name = re.sub(r'([A-Z])', r'_\1', name).lower()
    name = re.sub(r'\s+', '_', name)  # substitui espaços por _
    return name

def read_file_and_format_columns(file_bytes, file_name: str) -> pd.DataFrame:
    """Lê CSV ou Excel, remove BOM, converte colunas para snake_case e reindexa para COLUMNS."""
    if file_name.endswith(".csv"):
        df = pd.read_csv(file_bytes, sep=",", dtype=str, skipinitialspace=True)
    elif file_name.endswith((".xls", ".xlsx")):
        df = pd.read_excel(file_bytes, dtype=str)
    else:
        raise ValueError("Formato de arquivo não suportado")

    # Remove espaços e converte para snake_case
    df.columns = [re.sub(r'\s+', '_', col.strip().lower()) for col in df.columns]

    # Reindexa para garantir todas as colunas, adicionando NaN se faltar alguma
    df = df.reindex(columns=COLUMNS)

    return df

def fetch_html(url: str):
    response = requests.get(url)
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
