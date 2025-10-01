from io import BytesIO
from google.cloud import storage, bigquery
import pandas as pd
from datetime import date
from constants import (
	BUCKET_NAME,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME,
    COLUMNS
)

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
            print(f"📤 Arquivo {arquivo_bytes} enviado como {nome_no_bucket} para bucket {BUCKET_NAME}")
        else:
            # Se é BytesIO
            arquivo_bytes.seek(0)  # Garante que está no início
            blob.upload_from_file(arquivo_bytes)
            print(f"📤 Bytes enviados como {nome_no_bucket} para bucket {BUCKET_NAME}")
        
        return True
    except Exception as e:
        print(f"❌ Erro ao fazer upload para bucket: {e}")
        return False

def read_excel_from_bucket(nome_no_bucket, delivery_type=None):
    """
    Lê um arquivo Excel diretamente do bucket GCP e converte para DataFrame.
    Args:
        nome_no_bucket: Nome do arquivo no bucket
        delivery_type: Tipo do delivery ('SIM' ou 'NAO') para adicionar coluna identificadora
    Returns:
        pd.DataFrame ou None se falhar
    """
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(nome_no_bucket)
        
        # Baixa o arquivo para memória
        excel_bytes = BytesIO()
        blob.download_to_file(excel_bytes)
        excel_bytes.seek(0)
        
        # Lê o Excel da memória
        df = pd.read_excel(excel_bytes, engine='openpyxl')
        
        print(f"✅ Excel lido do bucket com sucesso: {len(df)} linhas, {len(df.columns)} colunas")
        return df
        
    except Exception as e:
        print(f"❌ Erro ao ler Excel do bucket: {e}")
        return None

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """ Insere dados no BigQuery com particionamento por data. """

    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    partition_key = date.today().strftime('%Y%m%d')

    partitioned_table_id = f"{table_id}${partition_key}"
    job = bq_client.load_table_from_dataframe(
         df, partitioned_table_id, job_config=job_config
    )
    job.result()

def format_columns_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    """ Renomeia colunas e ajusta tipos para BigQuery. """

    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)
    df = df.rename(columns=COLUMNS)
    df = df.astype(str)
    return df