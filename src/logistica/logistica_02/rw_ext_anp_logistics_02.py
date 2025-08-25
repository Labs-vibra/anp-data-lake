from io import BytesIO
from google.cloud import bigquery, storage
from constants import (
    MARKET_SALES_FILE_PATH,
    BUCKET_NAME,
    COLUMNS_MAPPING,
    LOGISTIC_02_TABLE_NAME
)
import pandas as pd

def get_file_bytes(file_name):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    file_bytes = blob.download_as_string()
    return BytesIO(file_bytes)

def insert_data_to_bigquery(df):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, LOGISTIC_02_TABLE_NAME, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {LOGISTIC_02_TABLE_NAME}.")

def process_market_sales():
    file_bytes = get_file_bytes(MARKET_SALES_FILE_PATH)
    df = pd.read_csv(file_bytes, sep=';', encoding="latin1", dtype=str)
    df.rename(columns=COLUMNS_MAPPING, inplace=True)
    insert_data_to_bigquery(df)

if __name__ == "__main__":
    process_market_sales()