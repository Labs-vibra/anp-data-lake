from argparse import Action
from io import BytesIO
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date
from google.cloud import bigquery
from utils import fetch_html, find_all_csv_links, download_file, normalize_column
from constants import BASE_URL, PROJECT_ID, BQ_DATASET, TABLE_NAME, COLUMNS
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """Insere dados no BigQuery com particionamento por data."""
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
    logger.info(f"{len(df)} registros inseridos no BigQuery.")

def get_export_file() -> bytes:
    """
    Simula o clique em 'Exportar com todos os participantes' e retorna
    o conteúdo do arquivo baixado (XLSX/CSV).
    """
    session = requests.Session()

    session = requests.Session()
    soup = fetch_html(BASE_URL)

    form = soup.find("form", {"id": "wwvFlowForm"})
    if not form:
        raise RuntimeError("Formulário principal não encontrado no HTML.")

    action = form["action"]
    if not action.startswith("http"):
        post_url = "https://cdp.anp.gov.br/ords/" + action
    else:
        post_url = action

    payload = {}
    for hidden in form.find_all("input", {"type": "hidden"}):
        name = hidden.get("name")
        value = hidden.get("value", "")
        if name:
            payload[name] = value

    payload["p_request"] = "Exportar_Participantes"

    resp = session.post(post_url, data=payload, verify=False)
    resp.raise_for_status()

    logger.info(f"Download concluído de {len(resp.content)} bytes")
    return resp.content

def build_raw() -> bool:
    """
    Baixa a exportação completa de 'Bases de Distribuição e TRR Autorizados',
    normaliza colunas e envia para BigQuery.
    """
    try:
        file_bytes = get_export_file()
        if not file_bytes:
            logger.error("Nenhum arquivo exportado foi retornado.")
            return False

        df = pd.read_excel(BytesIO(file_bytes), dtype=str, engine="openpyxl")

        df.columns = [normalize_column(c) for c in df.columns]

        for col in COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[COLUMNS]

        logger.info(f"Arquivo processado com {len(df)} registros.")

        insert_data_into_bigquery(df)
        logger.info("Raw 'Consulta Bases de Distribuição e TRR Autorizados' carregada com sucesso no BigQuery.")

        return True

    except Exception as e:
        logger.error(f"Erro ao processar exportação: {e}", exc_info=True)
        return False

def main():
    """
    Função principal que executa a extração dos dados de 'Consulta Bases de Distribuição
    e TRR Autorizados' e construção da raw no BQ.
    """
    logger.info("=== Iniciando raw de 'Consulta Bases de Distribuição e TRR Autorizados' ===")

    success = build_raw()

    if success:
        logger.info("=== Processo finalizado com sucesso! ===")
        exit(0)
    else:
        logger.error("=== Processo finalizado com erro! ===")
        exit(1)
    logger.info("=== Processo finalizado ===")

if __name__ == "__main__":
    main()

