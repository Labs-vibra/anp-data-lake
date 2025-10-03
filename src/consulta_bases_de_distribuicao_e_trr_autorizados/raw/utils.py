import re
import unicodedata
import logging
import os
from io import BytesIO
from google.cloud import bigquery, storage
from constants import BUCKET_NAME, BUCKET_PATH

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
