import io
import re
import pandas as pd
from datetime import date
from google.cloud import bigquery
from utils import fetch_html, find_data_links, download_file, normalize_column
from constants import URL_BASE, PROJECT_ID, BQ_DATASET, TABLE_NAME, COLUMNS
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

def build_raw() -> bool:
    """
    Baixa os dois CSVs consolidados de Dados Fiscalização do Abastecimento
    (1998-2018 e 2019-2025) e envia para BigQuery.
    """
    logger.info("Buscando HTML da página de Dados Fiscalização do Abastecimento...")
    soup = fetch_html(URL_BASE)
    links = find_data_links(soup)

    if not links:
        logger.error("Nenhum link .xlsx encontrado na página.")
        return False

    filtered_links = [
        link for link in links
        if "1998-2018" in link or "2019" in link
    ]

    if not filtered_links:
        logger.warning("Nenhum link dos blocos 1998-2018 ou 2019-2025 foi encontrado.")
        return False

    logger.info(f"{len(filtered_links)} arquivos serão processados.")

    all_dfs = []
    for link in filtered_links:
        try:
            file_bytes = download_file(link)

            if link.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(file_bytes.read()), dtype=str)
            else:
                df = pd.read_csv(file_bytes, sep=";", encoding="latin1", dtype=str)

            df.columns = [normalize_column(c) for c in df.columns]

            for col in COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA

            df = df[COLUMNS]

            all_dfs.append(df)
            logger.info(f"Arquivo {link} processado com {len(df)} registros.")
        except Exception as e:
            logger.warning(f"Erro ao processar {link}: {e}")

    if not all_dfs:
        logger.error("Nenhum CSV processado com sucesso.")
        return False

    df_all = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total de registros após concatenação: {len(df_all)}")

    insert_data_into_bigquery(df_all)
    logger.info("Raw Fiscalização do Abastecimento carregada com sucesso no BigQuery.")
    return True

def main():
    """
    Função principal que executa a extração dos dados de Fiscalização do Abastecimento
    Nacional de Combustíveis e construção da raw no BQ.
    """
    logger.info("=== Iniciando raw de Dados Fiscalização do Abastecimento ===")

    success = build_raw()

    if success:
        logger.info("=== Processo finalizado com sucesso! ===")
        exit(0)
    else:
        logger.error("=== Processo finalizado com erro! ===")
        exit(1)

if __name__ == "__main__":
    main()
