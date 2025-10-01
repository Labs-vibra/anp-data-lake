import re
import unicodedata
import logging
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_column(col: str) -> str:
    col = col.strip().lower()
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')
    col = re.sub(r'[^a-z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

def format_columns_for_bq(df):
    df = df.copy()
    df.columns = [normalize_column(c) for c in df.columns]
    return df

def insert_data_into_bigquery(df, dataset: str, table: str, project: str = None):
    client = bigquery.Client(project=project)
    table_ref = f"{dataset}.{table}" if not project else f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Data uploaded to BigQuery table {table_ref}, {df.shape[0]} rows")
        return True
    except Exception as e:
        logger.error(f"Failed to insert data into BigQuery: {e}")
        return False
