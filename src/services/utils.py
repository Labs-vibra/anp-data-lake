import os
import zipfile
import requests
from datetime import date
from io import BytesIO
from bs4 import BeautifulSoup

from google.cloud import storage
from pyspark.sql import SparkSession

from src.constants import BUCKET_NAME, RAW_PATH

def make_get_request(url: str) -> BeautifulSoup | None:
    """
    Realiza uma requisição GET e retorna o conteúdo parseado com BeautifulSoup.
    """
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.RequestException as e:
        print(f"[GET ERROR] {e}")
        return None

def download_file_to_memory(url: str) -> BytesIO | None:
    """
    Faz o download de um arquivo ZIP diretamente na memória.
    """
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.RequestException as e:
        print(f"[DOWNLOAD ERROR] {e}")
        return None

def unzip_in_memory(zip_bytes: BytesIO, extract_to: str) -> list[str]:
    """
    Descompacta um arquivo ZIP que está em memória.
    """
    extracted_files = []

    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    with zipfile.ZipFile(zip_bytes, "r") as zip_ref:
        zip_ref.extractall(extract_to)
        extracted_files = zip_ref.namelist()

    return [os.path.join(extract_to, f) for f in extracted_files]

def upload_file_to_gcs(local_file_path: str, bucket_name: str, dest_path: str):
    """
    Envia um arquivo local para o Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)

    blob.upload_from_filename(local_file_path)
    print(f"✔️ Upload feito para: gs://{bucket_name}/{dest_path}")

def set_date_partitioned_path(base_path: str, suffix: str = "") -> str:
    """
    Cria um caminho com ano/mês/dia baseado na data atual.
    """
    today = date.today()
    return os.path.join(
        base_path,
        str(today.year),
        f"{today.month:02}",
        f"{today.day:02}",
        suffix
    )

def read_csv_from_gcs(spark: SparkSession, path: str) -> 'DataFrame':
    """
    Lê um CSV do Cloud Storage via PySpark.
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
