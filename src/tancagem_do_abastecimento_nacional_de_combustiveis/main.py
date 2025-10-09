import re
import pandas as pd
from datetime import date
from google.cloud import bigquery
from utils import fetch_html, find_all_csv_links, download_file, read_file_and_format_columns
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

def build_raw(start_year: int, end_year: int) -> bool:
    """
    Baixa CSVs de Tancagem do Abastecimento Nacional de Combustíveis do site da ANP,
    normaliza colunas e envia para BigQuery.

    Parâmetros:
        start_year (int): ano inicial do intervalo
        end_year (int): ano final do intervalo
    """
    logger.info("Buscando HTML da página de Tancagem do Abastecimento Nacional de Combustíveis...")
    soup = fetch_html(URL_BASE)
    csv_links = find_all_csv_links(soup)

    if not csv_links:
        logger.error("Nenhum link CSV encontrado na página.")
        return False

    filtered_links = []
    for link in csv_links:
        match = re.search(r"(\d{4})", link)
        if match:
            year = int(match.group(1))
            if start_year and year < start_year:
                continue
            if end_year and year > end_year:
                continue
        filtered_links.append(link)

    if not filtered_links:
        logger.warning("Nenhum CSV dentro do intervalo de ano especificado.")
        return False

    logger.info(f"{len(filtered_links)} arquivos CSV serão processados.")

    all_dfs = []
    for link in filtered_links:
        try:
            file_bytes = download_file(link)
            df = read_file_and_format_columns(file_bytes, link)
            print("Colunas normalizadas:", df.columns.tolist())

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
    logger.info("Raw Tancagem do Abastecimento Nacional de Combustíveis carregada com sucesso no BigQuery.")
    return True

def main():
    """
    Função principal que executa a extração dos dados de Tancagem do Abastecimento
    Nacional de Combustíveis e construção da raw no BQ.
    """
    logger.info("=== Iniciando raw de Tancagem do Abastecimento Nacional de Combustíveis ===")

    success = build_raw(2022, 2025)

    if success:
        logger.info("=== Processo finalizado com sucesso! ===")
        exit(0)
    else:
        logger.error("=== Processo finalizado com erro! ===")
        exit(1)
    logger.info("=== Processo finalizado ===")

if __name__ == "__main__":
    main()
