import os
import zipfile
import requests
from datetime import date
from io import BytesIO

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