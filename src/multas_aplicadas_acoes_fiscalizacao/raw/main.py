import pandas as pd
from google.cloud import storage, bigquery
from constants import BUCKET_NAME, FILES_BUCKET_DESTINATION, BQ_TABLE
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mapeamento de colunas originais para padronizadas
COLUMN_MAPPING = {
    # 2016-2019
    "Status Processo": "status_processo",
    "Superintendência": "superintendencia",
    "Número do Processo": "numero_processo",
    "Auto de Infração": "auto_infracao",
    "CNPJ/CPF": "cnpj_cpf",
    "Razão Social": "razao_social",
    "Data Transito Julgado": "data_transito_julgado",
    "Vencimento": "vencimento",
    "Valor da Multa Aplicada": "valor_multa_aplicada",
    "Valor Total Pago (*) (**)": "valor_total_pago",
    # 2020
    " Valor  da Multa Aplicada": "valor_multa_aplicada",
    " Valor Total Pago (*) (**)": "valor_total_pago",
    # 2022
    "Valor da Multa": "valor_multa_aplicada",
    "Valor Total Pago": "valor_total_pago",
    # 2023
    "Superintendcia": "superintendencia",
    "Nσero do Processo": "numero_processo",
    "Nσero do DUF": "numero_duf",
    "Raz釅 Social": "razao_social",
    "Vencimento da Multa": "vencimento",
    " Valor da Multa": "valor_multa_aplicada",
    " Valor Total Pago": "valor_total_pago",
    # 2024
    "Número do DUF": "numero_duf",
}

# Arquivos e suas colunas esperadas
FILES_CONFIG = {
    "multasaplicadas2016a2019.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Auto de Infração", "CNPJ/CPF", "Razão Social",
            "Data Transito Julgado", "Vencimento",
            "Valor da Multa Aplicada", "Valor Total Pago (*) (**)"
        ],
        "ano_referencia": "2016-2019"
    },
    "multasaplicadas2020.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Auto de Infração", "CNPJ/CPF", "Razão Social",
            "Data Transito Julgado", "Vencimento",
            " Valor  da Multa Aplicada", " Valor Total Pago (*) (**)"
        ],
        "ano_referencia": "2020"
    },
    "multasaplicadas2022.csv": {
        "columns": [
            "Razão Social", "Data Transito Julgado", "Vencimento",
            "Valor da Multa", "Valor Total Pago"
        ],
        "ano_referencia": "2022"
    },
    "multasaplicadas2023.csv": {
        "columns": [
            "Status Processo", "Superintendcia", "Nσero do Processo",
            "Nσero do DUF", "CNPJ/CPF", "Raz釅 Social",
            "Vencimento da Multa", " Valor da Multa", " Valor Total Pago"
        ],
        "ano_referencia": "2023"
    },
    "multasaplicadas2024.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Número do DUF", "CNPJ/CPF", "Razão Social",
            "Vencimento", "Valor da Multa", "Valor Total Pago"
        ],
        "ano_referencia": "2024"
    }
}

# Schema padronizado final
STANDARDIZED_COLUMNS = [
    "status_processo",
    "superintendencia",
    "numero_processo",
    "auto_infracao",
    "numero_duf",
    "cnpj_cpf",
    "razao_social",
    "data_transito_julgado",
    "vencimento",
    "valor_multa_aplicada",
    "valor_total_pago",
    "ano_referencia",
    "arquivo_origem"
]


def download_and_process_csv(bucket_name: str, file_path: str, file_config: dict) -> pd.DataFrame:
    """Download e processa um arquivo CSV do GCS."""
    logger.info(f"Baixando arquivo: {file_path}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Download como string
    content = blob.download_as_string()

    # Lê CSV
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding='utf-8')

    # Renomeia colunas usando o mapeamento
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Adiciona colunas de metadados
    df['ano_referencia'] = file_config['ano_referencia']
    df['arquivo_origem'] = file_path.split('/')[-1]

    # Garante que todas as colunas padronizadas existam
    for col in STANDARDIZED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Seleciona apenas as colunas padronizadas
    df = df[STANDARDIZED_COLUMNS]

    # Converte valores monetários (remove símbolos e converte para float)
    if 'valor_multa_aplicada' in df.columns:
        df['valor_multa_aplicada'] = df['valor_multa_aplicada'].astype(str).str.replace('R$', '').str.replace('.', '').str.replace(',', '.').str.strip()
        df['valor_multa_aplicada'] = pd.to_numeric(df['valor_multa_aplicada'], errors='coerce')

    if 'valor_total_pago' in df.columns:
        df['valor_total_pago'] = df['valor_total_pago'].astype(str).str.replace('R$', '').str.replace('.', '').str.replace(',', '.').str.strip()
        df['valor_total_pago'] = pd.to_numeric(df['valor_total_pago'], errors='coerce')

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
