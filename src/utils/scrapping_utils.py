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