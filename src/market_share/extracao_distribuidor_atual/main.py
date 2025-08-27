from google.cloud import storage, bigquery
import pandas as pd
from io import BytesIO
from datetime import date
from constants import (
    file_name,
    BUCKET_NAME,
    DISTRIBUTOR_COLUMN_MAPPING,
    PROJECT_ID,
    BQ_DATASET,
    TABLE_NAME,
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(file_name)
data = blob.download_as_bytes()

logger.info(f"Downloaded file {file_name} from bucket {BUCKET_NAME}")
df = pd.read_csv(BytesIO(data), sep=";", encoding="latin-1", dtype=str)
df = df.rename(columns=DISTRIBUTOR_COLUMN_MAPPING)

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

logger.info(f"Loaded data into {partitioned_table_id} in BigQuery")