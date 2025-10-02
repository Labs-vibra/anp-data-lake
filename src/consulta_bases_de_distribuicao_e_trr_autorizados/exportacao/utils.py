import re
import unicodedata
import logging
import os
from io import BytesIO
from google.cloud import bigquery, storage
from constants import BUCKET_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_column(col: str) -> str:
    col = col.strip().lower()
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')
    col = re.sub(r'[^a-z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

def format_columns_for_bq(df):
    df = df.copy()
    df.columns = [normalize_column(c) for c in df.columns]
    return df

def insert_data_into_bigquery(df, dataset: str, table: str, project: str = None):
    client = bigquery.Client(project=project)
    table_ref = f"{dataset}.{table}" if not project else f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Data uploaded to BigQuery table {table_ref}, {df.shape[0]} rows")
        return True
    except Exception as e:
        logger.error(f"Failed to insert data into BigQuery: {e}")
        return False

def upload_bytes_to_bucket(arquivo_bytes, nome_no_bucket):
    """
    Faz upload de bytes para o bucket GCP.
    Args:
        arquivo_bytes: BytesIO ou caminho do arquivo local
        nome_no_bucket: Nome do arquivo no bucket
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(nome_no_bucket)
        
        if isinstance(arquivo_bytes, str):
            # Se é caminho de arquivo local
            blob.upload_from_filename(arquivo_bytes)
            logging.info(f"📤 Arquivo {arquivo_bytes} enviado como {nome_no_bucket} para bucket {BUCKET_NAME}")
        else:
            # Se é BytesIO
            arquivo_bytes.seek(0)  # Garante que está no início
            blob.upload_from_file(arquivo_bytes)
            logging.info(f"📤 Bytes enviados como {nome_no_bucket} para bucket {BUCKET_NAME}")
        
        return True
    except Exception as e:
        logging.error(f"❌ Erro ao fazer upload para bucket: {e}")
        return False

def processar_download(valor_select, pasta_download):
    """
    Processa o download de arquivos após o CAPTCHA ser aceito.
    Args:
        valor_select: Valor do select que foi processado
        pasta_download: Pasta local onde os arquivos foram baixados
    Returns:
        bool: True se sucesso, False caso contrário
    """
    try:
        # Verifica se há arquivos baixados na pasta
        if not os.path.exists(pasta_download):
            logging.info(f"❌ Pasta de download não encontrada: {pasta_download}")
            return False
        
        arquivos = os.listdir(pasta_download)
        arquivos_validos = []
        
        # Filtra apenas arquivos válidos (CSV, XLS, XLSX, etc.)
        extensoes_validas = ['.csv', '.xls', '.xlsx', '.zip', '.pdf']
        for arquivo in arquivos:
            if any(arquivo.lower().endswith(ext) for ext in extensoes_validas):
                arquivos_validos.append(arquivo)
        
        if not arquivos_validos:
            logging.info(f"❌ Nenhum arquivo válido encontrado para o valor '{valor_select}'")
            return False
        
        logging.info(f"📁 Encontrados {len(arquivos_validos)} arquivos para o valor '{valor_select}': {arquivos_validos}")
        
        # Upload de cada arquivo para o bucket
        for arquivo in arquivos_validos:
            caminho_local = os.path.join(pasta_download, arquivo)
            
            # Nome no bucket incluindo o valor do select
            nome_no_bucket = f"{BUCKET_PATH}{valor_select}_{arquivo}"
            
            # Faz upload
            sucesso = upload_bytes_to_bucket(caminho_local, nome_no_bucket)
            
            if sucesso:
                # Remove arquivo local após upload bem-sucedido
                try:
                    os.remove(caminho_local)
                    logging.info(f"🗑️ Arquivo local removido: {arquivo}")
                except Exception as e:
                    logging.warning(f"⚠️ Erro ao remover arquivo local: {e}")
            else:
                logging.error(f"❌ Falha no upload do arquivo: {arquivo}")
                return False
        
        logging.info(f"✅ Todos os arquivos do valor '{valor_select}' foram enviados para o bucket")
        return True
        
    except Exception as e:
        logging.error(f"❌ Erro ao processar downloads para o valor '{valor_select}': {e}")
        return False

def limpar_pasta_download(pasta_download):
    """
    Limpa todos os arquivos da pasta de download.
    Args:
        pasta_download: Caminho da pasta para limpar
    """
    try:
        if os.path.exists(pasta_download):
            for arquivo in os.listdir(pasta_download):
                caminho_arquivo = os.path.join(pasta_download, arquivo)
                if os.path.isfile(caminho_arquivo):
                    os.remove(caminho_arquivo)
            logging.info(f"🧹 Pasta de download limpa: {pasta_download}")
    except Exception as e:
        logging.warning(f"⚠️ Erro ao limpar pasta de download: {e}")

def configurar_downloads_chrome(pasta_download):
    """
    Configura as opções do Chrome para download automático.
    Args:
        pasta_download: Caminho da pasta de download
    Returns:
        webdriver.ChromeOptions: Opções configuradas
    """
    from selenium import webdriver
    
    chrome_options = webdriver.ChromeOptions()
    
    # Configurações básicas
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    
    # Configurações de download
    prefs = {
        "download.default_directory": pasta_download,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,  # Para baixar PDFs em vez de abrir no navegador
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    return chrome_options
