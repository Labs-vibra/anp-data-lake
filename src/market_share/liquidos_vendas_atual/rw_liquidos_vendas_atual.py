import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
from constants import (
    BUCKET_NAME,
    LIQUIDOS_VENDAS_ATUAL,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def format_columns_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    """ Renomeia colunas e ajusta tipos para BigQuery. """

    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)
    df = df.rename(columns={
        "Ano": "ano",
        "Mês": "mes",
        "Agente Regulado": "agente_regulado",
        "Código do Produto": "codigo_produto",
        "Nome do Produto": "nome_produto",
        "Descrição do Produto": "descricao_produto",
        "Região Origem": "regiao_origem",
        "UF Origem": "uf_origem",
        "Região Destinatário": "regiao_destinatario",
        "UF Destino": "uf_destino",
        "Mercado Destinatário": "mercado_destinatario",
        "Quantidade de Produto (mil m³)": "quantidade_produto"
    })
    df = df.astype(str)
    return df

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
    Faz download do arquivo de Liquidos de vendas atual do bucket no GCP,
    lê arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(LIQUIDOS_VENDAS_ATUAL)

    logging.info(f"Baixando arquivo {LIQUIDOS_VENDAS_ATUAL} do bucket {BUCKET_NAME}...")
    data_bytes = blob.download_as_bytes()

    df = pd.read_csv(BytesIO(data_bytes), sep=";", encoding="latin1")
    logging.info(f"Arquivo carregado com {len(df)} registros.")

    df = format_columns_for_bq(df)

    insert_data_into_bigquery(df)
    logging.info("Inserção de dados concluída.")

if __name__ == "__main__":
	rw_ext_anp_liquidos_vendas_atual()
