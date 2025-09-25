import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import requests
from bs4 import BeautifulSoup
from utils import get_latest_links(), normalize_column()
import logging
from constants import{
    BQ_DATASET,
    PROJECT_ID,
    BUCKET_NAME
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def insert_data_into_bigquery(df: pd.DataFrame , name) -> None:
    """ Insere dados no BigQuery com particionamento por data. """

    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{name}"

    job_config = bigquery.LoadJobConfig(
         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    partition_key = date.today().strftime('%Y%m%d')

    partitioned_table_id = f"{table_id}${partition_key}"
    job = bq_client.load_table_from_dataframe(
         df, partitioned_table_id, job_config=job_config
    )
    job.result()

def extracao():

    links = get_latest_links()
    if not links:
        raise FileNotFoundError("Nenhum CSV encontrado no site da ANP.")

    # Exemplo: separar pelo nome (um geral e outro por região)
    link_geral = [l for l in links if "2005" in l][-1]
    link_regiao = [l for l in links if not "2005" in l][-1]

    print(f"Baixando geral: {link_geral}")
    print(f"Baixando região: {link_regiao}")

    # --- CSV ---
    response_csv_geral = requests.get(link_geral)
    response_csv_geral.raise_for_status()

    df = pd.read_csv(BytesIO(response_csv_geral.content), sep=";", encoding="latin1")
    df.to_csv("producao_biodiesel_m3_geral.csv", index=False, sep=";", encoding="utf-8")
    print("Arquivo salvo com sucesso!")

    # --- CSV região ---
    response_csv_regiao = requests.get(link_regiao)
    response_csv_regiao.raise_for_status()

    dfr = pd.read_csv(BytesIO(response_csv_regiao.content), sep=";", encoding="latin1")
    dfr.to_csv("producao_biodiesel_m3_regiao.csv", index=False, sep=";", encoding="utf-8")
    print("Arquivo salvo com sucesso!")

    return df, dfr

if __name__ == "__main__":
    df, dfr = extracao()
    print(df.head())
    print(dfr.head())