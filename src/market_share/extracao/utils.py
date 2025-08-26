import datetime
import re
import zipfile
from io import BytesIO
from google.cloud import storage
from constants import BUCKET_NAME

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

    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    file_name = f"{timestamp}_{file_name}"
    file_name = file_name.upper()
    return file_name

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    print(f"Arquivo {arquivo_bytes} enviado como {nome_no_bucket}.")
    return True


def process_zip_and_upload_to_gcp(zip_bytes: BytesIO, dir_prefix: str):
    """Lê um ZIP em memória e envia arquivos filtrados para o bucket.

    Args:
        zip_bytes (BytesIO): Objeto em memória contendo o arquivo ZIP.
        dir_prefix (str): Prefixo/pasta no bucket onde os arquivos serão enviados.
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        for file_info in zf.infolist():
            source_file = zf.open(file_info)
            file_bytes = BytesIO(source_file.read())
            bucket_path = f"{dir_prefix}{process_file_name(file_info.filename)}"
            upload_bytes_to_bucket(file_bytes, bucket_path)
