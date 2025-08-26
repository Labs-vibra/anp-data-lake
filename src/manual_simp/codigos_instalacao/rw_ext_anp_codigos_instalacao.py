from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
from dotenv import load_dotenv
from constants import (
    BUCKET_NAME,
    CODIGOS_INSTALACAO_FILE,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def rw_ext_anp_codigos_instalacao():
    """
    Faz download do arquivo de Logística 1 do bucket no GCP,
    lê arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    load_dotenv()
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(CODIGOS_INSTALACAO_FILE)

    logging.info(f"Baixando arquivo {CODIGOS_INSTALACAO_FILE} do bucket {BUCKET_NAME}...")
    data_bytes = blob.download_as_bytes()

    df = pd.read_excel(BytesIO(data_bytes), engine="openpyxl", header=1, dtype=str)
    df.columns = df.columns.str.lower()
    df = df.rename(columns={"dataversao": "data_versao"})
    logging.info(f"Arquivo carregado com {len(df)} registros.")


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
    logging.info("Inserção de dados concluída.")

if __name__ == "__main__":
	rw_ext_anp_codigos_instalacao()
