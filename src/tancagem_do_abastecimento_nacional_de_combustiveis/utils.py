import re
import unicodedata
import pandas as pd
import requests
import openpyxl
from io import BytesIO
from bs4 import BeautifulSoup
from google.cloud import storage
import logging
from constants import URL_BASE, COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def camel_to_snake(name: str) -> str:
    """Converte CamelCase em snake_case e remove BOM e espaços extras."""
    name = str(name).strip().encode('utf-8').decode('utf-8-sig')
    name = name[0].lower() + name[1:] if name else name
    name = re.sub(r'([A-Z])', r'_\1', name).lower()
    name = re.sub(r'\s+', '_', name)  # substitui espaços por _
    return name

def normalize_column(col):
    """Remove BOM, strip, lower, replace spaces, convert CamelCase to snake_case."""
    col = str(col).strip().encode('utf-8').decode('utf-8-sig')
    col = re.sub(r'([a-z])([A-Z])', r'\1_\2', col)  # CamelCase to snake_case
    col = col.replace(" ", "_").replace("-", "_").lower()
    return col

def find_header_row_excel(file_bytes, expected_cols):
    """Detects the header row in an Excel file by searching for expected columns."""
    file_bytes.seek(0)
    wb = openpyxl.load_workbook(file_bytes, read_only=True)
    ws = wb.active
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        normalized = [normalize_column(str(cell)) for cell in row if cell]
        if sum(col in expected_cols for col in normalized) > len(expected_cols) // 2:
            return i
    return 0

def read_file_and_format_columns(file_bytes, file_name: str) -> pd.DataFrame:
    """
    Lê o arquivo (CSV ou Excel) e normaliza as colunas conforme COLUMNS. Retorna DataFrame vazio se
    não conseguir casar as colunas.
    """
    is_2022 = "2022" in file_name

    if file_name.endswith(".csv"):
        if is_2022:
            for sep in [",", ";"]:
                for skip in range(0, 5):
                    file_bytes.seek(0)
                    try:
                        df = pd.read_csv(file_bytes, sep=sep, dtype=str, skipinitialspace=True, encoding='utf-8-sig', skiprows=skip)
                    except UnicodeDecodeError:
                        file_bytes.seek(0)
                        df = pd.read_csv(file_bytes, sep=sep, dtype=str, skipinitialspace=True, encoding='latin1', skiprows=skip)
                    df.columns = [normalize_column(col) for col in df.columns]
                    df = df[[col for col in df.columns if col in COLUMNS]]
                    if len(df.columns) == len(COLUMNS):
                        df = df.reindex(columns=COLUMNS)
                        return df
            return pd.DataFrame(columns=COLUMNS)
        else:
            try:
                df = pd.read_csv(file_bytes, sep=",", dtype=str, skipinitialspace=True, encoding='utf-8-sig')
            except UnicodeDecodeError:
                file_bytes.seek(0)
                df = pd.read_csv(file_bytes, sep=",", dtype=str, skipinitialspace=True, encoding='latin1')
            df.columns = [normalize_column(col) for col in df.columns]
            df = df[[col for col in df.columns if col in COLUMNS]]
            df = df.reindex(columns=COLUMNS)

    elif file_name.endswith((".xls", ".xlsx")):
        try:
            df = pd.read_excel(file_bytes, dtype=str, engine='openpyxl')
        except Exception:
            try:
                df = pd.read_excel(file_bytes, dtype=str)
            except Exception as e:
                logger.error(f"Erro ao ler Excel: {e}")
                return pd.DataFrame(columns=COLUMNS)

        if len(df.columns) < 3 or any("unnamed" in str(c).lower() for c in df.columns):
            try:
                header_row = find_header_row_excel(file_bytes, COLUMNS)
                file_bytes.seek(0)
                df = pd.read_excel(file_bytes, dtype=str, engine='openpyxl', header=header_row)
            except Exception as e:
                logger.error(f"Erro ao tentar ajustar header do Excel: {e}")
                return pd.DataFrame(columns=COLUMNS)

        df.columns = [normalize_column(col) for col in df.columns]
        df = df[[col for col in df.columns if col in COLUMNS]]
        df = df.reindex(columns=COLUMNS)
        return df
    else:
        raise ValueError("Formato de arquivo não suportado")
    return df

def fetch_html(url: str):
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")

def find_all_csv_links(soup):
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".csv") or href.endswith(".xlsx"):
            links.append(a["href"])
    return links

def download_file(url: str) -> BytesIO:
    logger.info(f"Baixando arquivo: {url}")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    return BytesIO(response.content)

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_no_bucket)
    blob.upload_from_file(arquivo_bytes)
    logger.info(f"Arquivo enviado para bucket: {nome_no_bucket}")
    return True
