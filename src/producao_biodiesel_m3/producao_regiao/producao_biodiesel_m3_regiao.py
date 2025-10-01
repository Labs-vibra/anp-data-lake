import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import requests
from bs4 import BeautifulSoup
from utils import get_latest_links, normalize_column
from constants import (
#    BUCKET_NAME,
#    MARKET_SHARE_FOLDER,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME_REGIAO,
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """ Insere dados no BigQuery com particionamento por data. """

    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_REGIAO}"

    job_config = bigquery.LoadJobConfig(
         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    partition_key = date.today().strftime('%Y%m%d')

    partitioned_table_id = f"{table_id}${partition_key}"
    job = bq_client.load_table_from_dataframe(
         df, partitioned_table_id, job_config=job_config
    )
    job.result()

def rw_producao_biodiesel_regiao(): 
    """
    Faz download do arquivo produção de biodiesel m3 regiao do site anp,
    lê o arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    links = get_latest_links()
    if not links:
        raise FileNotFoundError("Nenhum CSV encontrado no site da ANP.")

    # Exemplo: separar pelo nome (um geral e outro por região)
    link = [l for l in links if not "2005" in l][-1]

    print(f"Baixando região: {link}")

    # --- CSV ---
    response_csv = requests.get(link)
    response_csv.raise_for_status()

    df = pd.read_csv(BytesIO(response_csv.content), sep=";", encoding="utf-8")
    print("Arquivo salvo com sucesso!")

    try:
        logging.info(f"Arquivo carregado com {len(df)} registros.")
        df.columns = [normalize_column(c) for c in df.columns]
        insert_data_into_bigquery(df)
        logging.info("Inserção de dados concluída.")
    except Exception as e:
        logger.warning(f"Erro ao processar {link}: {e}")

    return df

if __name__ == "__main__":
    df = rw_producao_biodiesel_regiao()

    print(df.head())
