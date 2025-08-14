import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
from constants import BUCKET_NAME, FILE_PATH, PROJECT_ID, BQ_DATASET, TABLE_NAME
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
        "Período": "periodo",
        "UF Origem": "uf_origem",
        "UF Destino": "uf_destino",
        "Produto": "produto",
        "Classificação": "classificacao",
        "Sub classificação": "sub_classificacao",
        "Operação": "operacao",
        "Modal": "modal",
        "Qtd Produto Líquido": "qtd_produto_liquido"
    })
    df['periodo'] = pd.to_datetime(df['periodo'], format='%Y/%m').dt.date
    df['qtd_produto_liquido'] = df['qtd_produto_liquido'].astype(float)
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
    try:
        job.result()
    except Exception as e:
        raise RuntimeError(f"Falha ao inserir dados para {partition_key}: {str(e)}")

    logging.info(f"Dados para {partition_key} inseridos com sucesso.")

def rw_ext_anp_logistics_01():
    """
    Faz download do arquivo de Logística 1 do bucket no GCP,
    lê arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    storage_client = storage.Client()
    file_name = FILE_PATH

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)

    if not blob.exists():
        raise RuntimeError(f"O arquivo {file_name} não foi encontrado no bucket {BUCKET_NAME}.")

    logging.info(f"Baixando arquivo {file_name} do bucket {BUCKET_NAME}...")
    data_bytes = blob.download_as_bytes()

    df = pd.read_csv(BytesIO(data_bytes), sep=";", encoding="latin1")
    logging.info(f"Arquivo carregado com {len(df)} registros.")

    df = format_columns_for_bq(df)

    insert_data_into_bigquery(df)
    logging.info("Inserção de dados concluída.")

if __name__ == "__main__":
	rw_ext_anp_logistics_01()
