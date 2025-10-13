import pandas as pd
from google.cloud import storage, bigquery
from constants import BUCKET_NAME, COLUMN_MAPPING, FILES_BUCKET_DESTINATION, BQ_TABLE, FILES_CONFIG, STANDARDIZED_COLUMNS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def download_and_process_csv(bucket_name: str, file_path: str, file_config: dict) -> pd.DataFrame:
    """Download e processa um arquivo CSV do GCS."""
    logger.info(f"Baixando arquivo: {file_path}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Download como string
    content = blob.download_as_string()

    # Lê CSV
    encoding = file_config.get("encoding", "utf-8")
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding=encoding, sep=';', header=file_config['header'], dtype=str)

    logger.info(f"Arquivo {file_path} baixado com {len(df)} registros")
    # Verifica se todas as colunas esperadas estão presentes
    logger.info(f"Colunas encontradas: {df.columns}")
    # Renomeia colunas usando o mapeamento
    df.rename(columns=file_config["columns"], inplace=True)
    # Adiciona colunas de metadados
    df['ano_referencia'] = file_config['ano_referencia']
    df['arquivo_origem'] = file_path.split('/')[-1]

    # Garante que todas as colunas padronizadas existam
    for col in STANDARDIZED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Seleciona apenas as colunas padronizadas
    df = df[STANDARDIZED_COLUMNS]

    logger.info(f"Colunas após renomeação: {df.columns}")
    # Converte todos os campos para STRING (padrão da camada raw)
    for col in df.columns:
        if col not in ['data_criacao']:
            df[col] = df[col].astype(str).replace('nan', None).replace('None', None)

    # Adiciona timestamp de criação
    df['data_criacao'] = pd.Timestamp.now()

    logger.info(f"Processado {len(df)} registros de {file_path}")

    return df


def load_to_bigquery(df: pd.DataFrame, table_id: str):
    """Carrega DataFrame no BigQuery."""
    logger.info(f"Carregando dados para {table_id}")

    bq_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Carregados {len(df)} registros em {table_id}")


def main():
    """Função principal do ETL."""
    logger.info("Iniciando processo ETL de multas aplicadas")

    all_dataframes = []

    # Processa cada arquivo
    for filename, config in FILES_CONFIG.items():
        file_path = f"{FILES_BUCKET_DESTINATION}/{filename}"

        try:
            df = download_and_process_csv(BUCKET_NAME, file_path, config)
            all_dataframes.append(df)
        except Exception as e:
            logger.error(f"Erro ao processar {filename}: {str(e)}")
            continue

    # Concatena todos os DataFrames
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Total de {len(final_df)} registros para carregar")

        # Carrega no BigQuery
        load_to_bigquery(final_df, BQ_TABLE)

        logger.info("Processo ETL concluído com sucesso")
    else:
        logger.warning("Nenhum dado foi processado")


if __name__ == "__main__":
    main()
