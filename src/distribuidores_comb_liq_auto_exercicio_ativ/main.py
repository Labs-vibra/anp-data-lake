import pandas as pd
import requests
from io import StringIO
import unicodedata
import re
import logging
from datetime import date
from google.cloud import bigquery
from constants import PROJECT_ID, BQ_DATASET, TABLE_NAME, ANP_URL

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def remove_accents(text):
    """Remove acentos de uma string"""
    if pd.isna(text):
        return text
    text = str(text)
    # Normaliza para NFD (decomposição canônica)
    text = unicodedata.normalize('NFD', text)
    # Remove caracteres de combinação (acentos)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    return text

def clean_column_name(column_name):
    """Limpa o nome da coluna: remove acentos, substitui espaços por underscore"""
    # Remove acentos
    clean_name = remove_accents(column_name)
    # Substitui espaços por underscore
    clean_name = clean_name.replace(' ', '_')
    # Remove caracteres especiais (mantém apenas letras, números e underscore)
    clean_name = re.sub(r'[^\w]', '_', clean_name)
    # Remove underscores múltiplos
    clean_name = re.sub(r'_+', '_', clean_name)
    # Remove underscores no início e fim
    clean_name = clean_name.strip('_')
    # Converte para minúsculo
    clean_name = clean_name.lower()
    return clean_name

def insert_data_into_bigquery(df: pd.DataFrame) -> None:
    """Insere dados no BigQuery com timestamp de inserção."""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        # Adiciona timestamp de inserção
        df_with_timestamp = df.copy()
        df_with_timestamp['data_insercao'] = pd.Timestamp.now()

        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"

        # Configuração do job de inserção
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False
        )

        logger.info(f"Inserindo {len(df_with_timestamp)} registros na tabela {TABLE_NAME}...")

        # Executa a inserção
        job = bq_client.load_table_from_dataframe(
            df_with_timestamp, table_id, job_config=job_config
        )
        job.result()

        logger.info(f"Dados inseridos com sucesso na tabela {TABLE_NAME}")
    except Exception as e:
        logger.error(f"Erro ao inserir dados no BigQuery: {e}")
        raise

def download_and_process_csv():
    """Baixa e processa o CSV da ANP"""
    url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/dcl/planilha-aea-filiais.csv"

    try:
        logger.info("Baixando CSV da ANP...")
        response = requests.get(url)
        response.raise_for_status()

        content = response.content.decode('latin-1')

        df = pd.read_csv(StringIO(content), sep=';')
        logger.info("CSV processado com encoding latin-1")

        logger.info(f"CSV carregado com sucesso! Shape: {df.shape}")
        logger.info(f"Colunas originais: {list(df.columns)}")

        # Renomeia as colunas
        original_columns = df.columns.tolist()
        new_columns = [clean_column_name(col) for col in original_columns]
        column_mapping = dict(zip(original_columns, new_columns))
        df = df.rename(columns=column_mapping)

        return df

    except requests.RequestException as e:
        logger.error(f"Erro ao baixar o arquivo: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        return None

if __name__ == "__main__":
    df = download_and_process_csv()

    if df is not None:
        logger.info("=== PROCESSAMENTO CONCLUÍDO ===")
        logger.info(f"Total de registros: {len(df)}")
        logger.info(f"Total de colunas: {len(df.columns)}")

        logger.info("=== PRIMEIRAS 5 LINHAS ===")
        logger.info(f"\n{df.head()}")

        logger.info("=== INFORMAÇÕES DAS COLUNAS ===")
        for i, col in enumerate(df.columns):
            logger.info(f"{i+1:2d}. {col}")

        # Inserir dados no BigQuery
        try:
            logger.info("=== INSERINDO DADOS NO BIGQUERY ===")
            insert_data_into_bigquery(df)
            logger.info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")
        except Exception as e:
            logger.error(f"Erro na inserção no BigQuery: {e}")
            logger.error("Pipeline finalizado com erro")
    else:
        logger.error("Falha no processamento do arquivo.")