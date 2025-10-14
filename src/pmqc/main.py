import re
import pandas as pd
from datetime import date
from google.cloud import bigquery
from utils import fetch_html, find_all_csv_links, download_file, normalize_column
from constants import URL_BASE, PROJECT_ID, BQ_DATASET, TABLE_NAME, COLUMNS
import logging
import argparse

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

def build_pmqc_raw(start_year: int, end_year: int):
    """
    Baixa CSVs de PMQC do site da ANP, normaliza colunas e envia para BigQuery.

    Parâmetros:
        start_year (int): ano inicial do intervalo (inclusive)
        end_year (int): ano final do intervalo (inclusive)
    """
    logger.info("Buscando HTML da página de PMQC...")
    soup = fetch_html(URL_BASE)
    csv_links = find_all_csv_links(soup)

    if not csv_links:
        logger.error("Nenhum link CSV encontrado na página.")
        return

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
        return

    logger.info(f"{len(filtered_links)} arquivos CSV serão processados.")

    all_dfs = []
    for link in filtered_links:
        try:
            file_bytes = download_file(link)
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
        return

    df_all = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total de registros após concatenação: {len(df_all)}")

    insert_data_into_bigquery(df_all)
    logger.info("Raw PMQC carregada com sucesso no BigQuery.")

def main():
    """
    Função principal que executa a extração dos dados de PMQC e construção da raw no BQ.
    Aceita argumentos --start_year e --end_year para processar anos específicos.
    """

    parser = argparse.ArgumentParser(description="Extração de dados PMQC")
    parser.add_argument("--start_year", type=int, default=2016, help="Ano inicial (padrão: 2016)")
    parser.add_argument("--end_year", type=int, default=2025, help="Ano final (padrão: 2025)")
    args = parser.parse_args()
    
    logger.info(f"=== Iniciando raw de PMQC para os anos {args.start_year} a {args.end_year} ===")

    build_pmqc_raw(args.start_year, args.end_year)
    logger.info("=== Processo finalizado com sucesso! ===")

if __name__ == "__main__":
    main()
