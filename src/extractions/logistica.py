"""
    Extract Logistics data from ANP page, downloads the ZIP file if available, and save it in BiqQuery.
"""

import re
import os
import requests
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
from datetime import date
from google.cloud import bigquery

URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp/" \
"paineis-dinamicos-do-abastecimento/painel-dinamico-da-logistica-do-abastecimento-nacional-de-combustiveis"

response = requests.get(URL, verify=False)
if response.status_code == 200:
    page = BeautifulSoup(response.content, "html.parser")
else:
    print(f"Failed to retrieve data: {response.status_code}")

pattern = re.compile(r"Consulte\s+aqu.*Logística", re.IGNORECASE)
a_tag = page.find('a', string=pattern)
if a_tag and 'href' in a_tag.attrs:

    link = a_tag['href']
    text = a_tag.get_text(strip=True)
    li_text = a_tag.get_text(strip=True)

    panel = {
        'text': text,
        'link': link,
    }
else:
    panel = None

data_link = page.find('a', string='Veja também a base dados do painel')
if data_link and 'href' in data_link.attrs:
    link = data_link['href']
    text = data_link.get_text(strip=True)

    updated_data = data_link.next_sibling
    if updated_data:
        li_text = updated_data.get_text(strip=True).split("em ")[1][0:-1].strip()
    else:
        li_text = "Data não disponível"

    data = {
        'text': text,
        'link': link,
        'updated_date': li_text
    }
else:
    data = None

if data and data.get('link'):
    file_link_to_upload = data.get('link')

    response = requests.get(file_link_to_upload, verify=False)
    response.raise_for_status()
    zip_bytes = BytesIO(response.content)
    file_name = "logistics.zip"
else:
    print("No data link found.")

# """
#     Read the logistics sales data from a zip file and convert it to a DataFrame.
# """

# def process_zip_files(zip_bytes):
#     target_files = [
#         "DADOS ABERTOS - LOGISTICA 01 - ABASTECIMENTO NACIONAL DE COMBUST÷VEIS",
#         "DADOS ABERTOS - LOGISTICA 02 - VENDAS NO MERCADO BRASILEIRO DE COMBUST÷VEIS",
#         "DADOS ABERTOS - LOGISTICA 03 - VENDAS CONG╥NERES DE DISTRIBUIDORES.csv",
#     ]
#     processed_files = []

#     with zipfile.ZipFile(zip_bytes) as zf:
#         for file_info in zf.infolist():
#             file_name = file_info.filename.upper()
#             if any(target in file_name for target in target_files):
#                 with zf.open(file_info) as file:
#                     df = pd.read_csv(file, sep=";", encoding="latin1")
#                     processed_files.append((file_info.filename, df))
#     return processed_files

# """
#     Process the DataFrame to rename columns and convert data types.
# """
