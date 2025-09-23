import io
from io import BytesIO
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import requests
#from constants import (
#    BUCKET_NAME,
#    MARKET_SHARE_FOLDER,
#    PROJECT_ID,
#    BQ_DATASET,
#    TABLE_NAME,
#    MAPPING_COLUMNS
#)
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

def rw_ext_anp_producao_biodiesel_barris():
    """
    Faz download do arquivo Liquidos_Vendas_Atuais.csv mais recente do bucket no GCP,
    lê o arquivo, formata colunas e sobe a camada raw para o BigQuery.
    """
    #storage_client = storage.Client()

    #bucket = storage_client.bucket(BUCKET_NAME)
    
    #blobs = list(bucket.list_blobs(prefix=MARKET_SHARE_FOLDER))
    #lobs_filtrados = [b for b in blobs if "producao_biodiesel_barris" in b.name]
    #if not blobs_filtrados:
    #    raise FileNotFoundError("Nenhum arquivo encontrado com 'producao_biodiesel_barris' no nome.")
    #latest_blob = max(blobs_filtrados, key=lambda b: b.updated)

    #logging.info(f"Baixando arquivo {latest_blob.name} do bucket {BUCKET_NAME}...")
    #data_bytes = latest_blob.download_as_bytes()

    #url_xls = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/pb/producao-biodiesel-b.xls"
    url_csv = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2005-2022.csv"

    # --- XLS ---
#    response_xls = requests.get(url_xls)
#    response_xls.raise_for_status()

#    df_xls = pd.read_excel(BytesIO(response_xls.content))
#    df_xls.to_csv("producao_biodiesel_barris.csv", index=False, sep=";", encoding="utf-8")

    # --- CSV ---
    response_csv = requests.get(url_csv)
    response_csv.raise_for_status()

    df_csv = pd.read_csv(BytesIO(response_csv.content), sep=";", encoding="latin1")
    df_csv.to_csv("producao_biodiesel_m3.csv", index=False, sep=";", encoding="utf-8")

    print("Arquivos salvos com sucesso!")
    #df.rename(columns=MAPPING_COLUMNS, inplace=True)
    return df_xls
    #insert_data_into_bigquery(df)
    #logging.info("Inserção de dados concluída.")


if __name__ == "__main__":
    df = rw_ext_anp_producao_biodiesel_barris()
    print(df)