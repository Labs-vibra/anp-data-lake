import logging
from io import BytesIO
import pandas as pd
from google.cloud import storage
from constants import BUCKET_NAME, BUCKET_PATH, PROJECT_ID, BQ_DATASET, TABLE_NAME, COLUMNS
from utils import format_columns_for_bq
from datetime import date
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """Insere dados no BigQuery com particionamento por data."""
    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    partition_key = date.today().strftime('%Y%m%d')
    partitioned_table_id = f"{table_id}${partition_key}"

    job = bq_client.load_table_from_dataframe(df, partitioned_table_id, job_config=job_config)
    job.result()
    logging.info(f"Dados carregados no BigQuery ({len(df)} registros).")

def rw_ext_consulta_bases_de_distribuicao_e_trr_autorizados():
    """
    Baixa os 6 arquivos de Consulta Bases de Distribuição e TRR Autorizados do bucket,
    concatena todos em um único DataFrame, normaliza colunas e insere na camada raw do BigQuery.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix=BUCKET_PATH))
    xlsx_files = [b for b in blobs if b.name.endswith(".xlsx")]

    logging.info(f"{len(xlsx_files)} arquivos serão processados.")

    all_dfs = []
    for blob in xlsx_files:
        try:
            file_bytes = blob.download_as_bytes()
            df = pd.read_excel(BytesIO(file_bytes), dtype=str)

            df = format_columns_for_bq(df)

            for col in COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA

            df = df[COLUMNS]

            all_dfs.append(df)
            logging.info(f"Arquivo {blob.name} processado com {len(df)} registros.")
        except Exception as e:
            logging.warning(f"Erro ao processar {blob.name}: {e}")

    if not all_dfs:
        logging.warning("Nenhum arquivo foi processado com sucesso.")
        return

    df_final = pd.concat(all_dfs, ignore_index=True)
    logging.info(f"Concatenação concluída: {len(df_final)} registros.")

if __name__ == "__main__":
    rw_ext_consulta_bases_de_distribuicao_e_trr_autorizados()
