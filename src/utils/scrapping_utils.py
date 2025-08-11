import os
import re
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from services.gcp.gcs import upload_bytes_to_bucket
from  utils.constants import LOGISTICS_CSV_FILENAME_KEYWORD, LOGISTICS_CSV_ALLOWED_NUMBERS, LOGISTICS_CSV_EXTENSION

def fetch_html(url):
    """
    Faz request GET e retorna
    BeautifulSoup do conteúdo HTML.

    Args:
        url (str): URL da página HTML a ser baixada.

    Returns:
        BeautifulSoup: Objeto BeautifulSoup contendo o HTML da página.

    Raises:
        Exception: Se a resposta HTTP não for 200.
    """

    response = requests.get(url, verify=False)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a URL ({url}): {response.status_code}")
    return BeautifulSoup(response.content, "html.parser")

def find_link_by_text(soup, link_text):
    """
    Busca link cujo texto é exatamente link_text.
    Retorna dict com texto, link e updated_date, ou None.

    Args:
        soup (BeautifulSoup): Objeto BeautifulSoup para buscar o link.
        link_text (str): Texto exato do link a ser encontrado.

    Returns:
        dict or None: Dicionário com as chaves 'text', 'link' e 'updated_date' se encontrado,
                      caso contrário None.
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

def process_zip_and_upload_to_gcp(zip_bytes: BytesIO, dir_prefix: str):
    """Lê um ZIP em memória e envia arquivos filtrados para o bucket.

    Args:
        zip_bytes (BytesIO): Objeto em memória contendo o arquivo ZIP.
        dir_prefix (str): Prefixo/pasta no bucket onde os arquivos serão enviados.
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        for file_info in zf.infolist():
            file_name_upper = file_info.filename.upper()
            if is_target_csv(file_info.filename):
                with zf.open(file_info) as source_file:
                    file_bytes = BytesIO(source_file.read())
                    bucket_path = f"{dir_prefix}{file_info.filename}"
                    upload_bytes_to_bucket(file_bytes, bucket_path)

def is_target_csv(filename: str) -> bool:
    """Verifica se o arquivo atende aos critérios de seleção.

    Args:
        filename (str): Nome do arquivo para checagem.

    Returns:
        bool: True se o arquivo atende aos critérios, False caso contrário.
    """
    file_upper = filename.upper()
    return (
        LOGISTICS_CSV_FILENAME_KEYWORD in file_upper
        and any(num in file_upper for num in LOGISTICS_CSV_ALLOWED_NUMBERS)
        and file_upper.endswith(LOGISTICS_CSV_EXTENSION)
    )
