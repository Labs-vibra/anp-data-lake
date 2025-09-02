import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
from constants import (
    BUCKET_NAME,
    MARKET_SHARE_FOLDER,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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

def rw_ext_anp_liquidos_vendas_atual():
    """
    Faz download do arquivo Liquidos_Importacao_de_Distribuidores.csv mais recente do bucket no GCP,
    lê o arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    
    blobs = list(bucket.list_blobs(prefix=MARKET_SHARE_FOLDER))
    blobs_filtrados = [b for b in blobs if "LIQUIDOS_VENDAS_ATUAL" in b.name]
    if not blobs_filtrados:
        raise FileNotFoundError("Nenhum arquivo encontrado com 'LIQUIDOS_VENDAS_ATUAL' no nome.")
    latest_blob = max(blobs_filtrados, key=lambda b: b.updated)

    logging.info(f"Baixando arquivo {latest_blob.name} do bucket {BUCKET_NAME}...")
    data_bytes = latest_blob.download_as_bytes()

    df = pd.read_csv(BytesIO(data_bytes), sep=";", encoding="latin1", dtype=str)

    df.rename(columns=MAPPING_COLUMNS, inplace=True)

    insert_data_into_bigquery(df)
    logging.info("Inserção de dados concluída.")

if __name__ == "__main__":
	rw_ext_anp_liquidos_vendas_atual()
