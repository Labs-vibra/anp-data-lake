import datetime
import re
import zipfile
import requests
import logging
from io import BytesIO
from bs4 import BeautifulSoup
from google.cloud import storage
from constants import BUCKET_NAME

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    print(f"Arquivo {arquivo_bytes} enviado como {nome_no_bucket}.")
    return True
