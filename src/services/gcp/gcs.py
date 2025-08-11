from google.cloud import storage
import requests
from io import BytesIO
import zipfile
import os
from dotenv import load_dotenv
from src.utils.constants import BUCKET_NAME

load_dotenv()

bucket_name = os.getenv("GOOGLE_BUCKET_NAME", BUCKET_NAME)

def upload_file_to_bucket(arquivo_local, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_filename(arquivo_local)
    print(f"Arquivo {arquivo_local} enviado como {nome_no_bucket}.")

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    print(f"Arquivo {arquivo_bytes} enviado como {nome_no_bucket}.")
    return True

def download_from_gcs(nome_no_bucket, destino_local):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_no_bucket)
    blob.download_to_filename(destino_local)
    return True

def listar_arquivos(folder = None):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=folder or None)
    return

def upload_file_from_url(url, path=None):
    response = requests.get(url, verify=False)
    file_name = url.split("/")[-1]
    if response.status_code != 200:
        raise Exception(f"Falha ao baixar o arquivo: {response.status_code}")

    file_bytes = response.content
    file = BytesIO(file_bytes)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    final_path = path + file_name if path else file_name
    blob = bucket.blob(final_path)
    blob.upload_from_file(file)
    print(f"Arquivo {file_name} enviado como {final_path}.")

    return final_path

def get_file_bytes(file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_bytes = blob.download_as_string()
    return file_bytes

def upload_zip_file(url, path=None):
    response = requests.get(url, verify=False)
    response.raise_for_status()

    zip_bytes = BytesIO(response.content)
    file_name = "logistics.zip"
    if path:
        file_name = path + file_name
    upload_bytes_to_bucket(zip_bytes, file_name)
    return file_name

def unzip_gcs_file(file_name, path=None):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    zip_bytes = blob.download_as_string()

    files = []

    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        for file_info in zf.infolist():
            with zf.open(file_info) as file:
                file_name = file_info.filename
                final_path = path + file_name if path else file_name
                blob = storage.Blob(final_path, bucket)
                blob.upload_from_file(file)
                files.append(final_path)
    return files

def unzip_file_and_upload(url, path=None):
    response = requests.get(url, verify=False)
    response.raise_for_status()

    zip_bytes = BytesIO(response.content)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    files = []
    with zipfile.ZipFile(zip_bytes) as zf:
        for file_info in zf.infolist():
            with zf.open(file_info) as file:
                file_name = file_info.filename
                final_path = path + file_name if path else file_name
                blob = storage.Blob(final_path, bucket)
                blob.upload_from_file(file)
                files.append(final_path)
    return files
