import os
import re
import zipfile
import requests
from datetime import date
from io import BytesIO
from bs4 import BeautifulSoup

def fetch_html(url):
    """
    Faz request GET e retorna
    BeautifulSoup do conteúdo HTML.
    """

    response = requests.get(url, verify=False)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a URL ({url}): {response.status_code}")
    return BeautifulSoup(response.content, "html.parser")

def find_link_by_pattern(soup, pattern):
    """
    Busca link cujo texto bate com regex.
    Retorna dict com texto e link ou None.
    """

    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    a_tag = soup.find('a', string=compiled_pattern)
    if a_tag and 'href' in a_tag.attrs:
        return {
            'text': a_tag.get_text(strip=True),
            'link': a_tag['href']
        }
    return None

def find_link_by_text(soup, link_text):
    """
    Busca link cujo texto é exatamente link_text.
    Retorna dict com texto, link e updated_date, ou None.
    """

    data_link = soup.find('a', string=link_text)
    if data_link and 'href' in data_link.attrs:
        updated_data = data_link.next_sibling
        updated_date = "Data não disponível"
        if updated_data:
            try:
                updated_date = updated_data.get_text(strip=True).split("em ")[1][0:-1].strip()
            except IndexError:
                pass
        return {
            'text': data_link.get_text(strip=True),
            'link': data_link['href'],
            'updated_date': updated_date
        }
    return None

def download_file(url):
    """Faz download de arquivo e retorna em BytesIO."""

    response = requests.get(url, verify=False)
    response.raise_for_status()
    return BytesIO(response.content)

def save_zip_file(zip_bytes, save_path):
    """Salva conteúdo em BytesIO como arquivo zip localmente."""

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, 'wb') as f:
        f.write(zip_bytes.read())
    print(f"ZIP salvo em: {save_path}")

def extract_target_files_from_zip(zip_path, output_dir, file_pattern_key):
    """
    Extrai arquivos do ZIP local que batem com patterns definidos em FILE_PATTERNS.
    Salva os arquivos extraídos em output_dir.
    """

    os.makedirs(output_dir, exist_ok=True)
    target_patterns = FILE_PATTERNS.get(file_pattern_key, [])

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for file_info in zf.infolist():
            file_name_upper = file_info.filename.upper()
            if any(pattern in file_name_upper for pattern in target_patterns):
                output_file_path = os.path.join(output_dir, file_info.filename)
                with zf.open(file_info) as source_file, open(output_file_path, 'wb') as dest_file:
                    dest_file.write(source_file.read())
                print(f"Arquivo extraído: {output_file_path}")

# def upload_file_to_gcs(local_file_path: str, bucket_name: str, dest_path: str):
#     """
#     Envia um arquivo local para o Cloud Storage.
#     """

#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(dest_path)

#     blob.upload_from_filename(local_file_path)
#     print(f"Upload feito para: gs://{bucket_name}/{dest_path}")