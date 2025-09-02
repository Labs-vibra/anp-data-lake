import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
from constants import (
    BUCKET_NAME,
    MARKET_SHARE_FOLDER,
    COLUMNS,
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

def rw_ext_anp_liquidos_historico_vendas():
    """
    Faz download do arquivo LIQUIDOS_VENDAS_HISTORICO_2007_A_2017 mais recente do bucket no GCP,
    lê o arquivo, formata colunas e sobe a camada raw para o BigQuery.

    Particularidade do arquivo:
    - O CSV não possui header (nomes das colunas).
    - A primeira linha contém os nomes antigos das colunas e precisa ser removida.
    - A tabela final possui 10 colunas, em ordem:
        ano, mes, distribuidor, codigo_produto, nome_produto,
        regiao_origem, uf_origem, regiao_destinatario, uf_destino, quantidade_produto_mil_m3
    """
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix=MARKET_SHARE_FOLDER))
    blobs_filtrados = [b for b in blobs if "LIQUIDOS_VENDAS_HISTORICO_2007_A_2017" in b.name]
    if not blobs_filtrados:
        raise FileNotFoundError("Nenhum arquivo encontrado com 'LIQUIDOS_VENDAS_HISTORICO_2007_A_2017' no nome.")
    latest_blob = max(blobs_filtrados, key=lambda b: b.updated)

    logging.info(f"Baixando arquivo {latest_blob.name} do bucket {BUCKET_NAME}...")
    data_bytes = latest_blob.download_as_bytes()

    # Lê CSV sem header
    df = pd.read_csv(BytesIO(data_bytes), sep=";", encoding="latin1", header=None, dtype=str)

    # Remove coluna extra se existir
    if df.shape[1] > 10:
        df = df.iloc[:, :10]

    df.columns = COLUMNS

    # Remove primeira linha que tinha nomes antigos
    df = df.iloc[1:].reset_index(drop=True)

    logging.info(f"Arquivo carregado com {len(df)} registros.")

    insert_data_into_bigquery(df)
    logging.info("Inserção de dados concluída.")

if __name__ == "__main__":
	rw_ext_anp_liquidos_historico_vendas()
