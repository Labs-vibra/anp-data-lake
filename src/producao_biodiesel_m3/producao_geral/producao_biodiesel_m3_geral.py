import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import requests
from bs4 import BeautifulSoup
from utils import get_latest_links, normalize_column
from constants import (
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME_GERAL,
    MAPPING_COLUM
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """ Insere dados no BigQuery com particionamento por data. """

    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_GERAL}"

    job_config = bigquery.LoadJobConfig(
         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    partition_key = date.today().strftime('%Y%m%d')

    partitioned_table_id = f"{table_id}${partition_key}"
    job = bq_client.load_table_from_dataframe(
         df, partitioned_table_id, job_config=job_config
    )
    job.result()

def rw_producao_biodiesel_geral(): 
    """
    Faz download do arquivo produção de biodiesel m3 geral do site anp,
    lê o arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    links = get_latest_links()
    if not links:
        raise FileNotFoundError("Nenhum CSV encontrado no site da ANP.")

    link = [l for l in links if "2005" in l][-1]

    print(f"Baixando geral: {link}")

    # --- CSV ---
    response_csv = requests.get(link)
    response_csv.raise_for_status()

    df = pd.read_csv(BytesIO(response_csv.content), sep=";", encoding="utf-8")
    logging.info(f"Arquivo carregado com {len(df)} registros.")
    print("Arquivo salvo com sucesso!")

    try:
        logging.info(f"Arquivo carregado com {len(df)} registros.")
        df.columns = [normalize_column(c) for c in df.columns]
        df["ano"] = df["ano"].astype(str)
        logging.info("Inserção de dados concluída.")

        df.rename(columns=MAPPING_COLUM, inplace=True)
    except Exception as e:
        logging.warning(f"Erro ao processar {link}: {e}")
    
    insert_data_into_bigquery(df)

if __name__ == "__main__":
    rw_producao_biodiesel_geral()
