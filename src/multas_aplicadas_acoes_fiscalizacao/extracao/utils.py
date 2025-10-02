import re
import requests
from io import BytesIO
from google.cloud import storage
from constants import BUCKET_NAME

def download_file(url):
    """
    Faz download de arquivo e retorna em BytesIO.

    Args:
        url (str): URL do arquivo a ser baixado.

    Returns:
        BytesIO: Conteúdo do arquivo em um objeto BytesIO.

    Raises:
        requests.HTTPError: Se o download falhar (status diferente de 200).
    """
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return BytesIO(response.content)

def process_file_name(file_name: str) -> str:
    """
    Processa o nome do arquivo para ser usado no bucket.
    retira caracteres especiais e formata o nome.

    Args:
        file_name (str): Nome original do arquivo.

    Returns:
        str: Nome processado para ser usado no bucket.
    """
    substitutions = {
        "╓": "I",
    }
    for old, new in substitutions.items():
        file_name = file_name.replace(old, new)

    file_name = re.sub(r"\s+", " ", file_name)
    file_name = file_name.replace(" ", "_")
    file_name = re.sub(r"[^a-zA-Z0-9_.]", "", file_name)
    file_name = re.sub(r"_+", "_", file_name)
    return file_name

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    print(f"Arquivo {arquivo_bytes} enviado como {nome_no_bucket}.")
    return True
