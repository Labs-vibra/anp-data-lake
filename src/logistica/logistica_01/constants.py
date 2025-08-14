import os

BUCKET_NAME="ext-ecole-biomassa"

#RAW Logística 1
LOGISTICS_01_FILE = "anp/logistica/DADOS_ABERTOS_LOGISTICA_01_ABASTECIMENTO_NACIONAL_DE_COMBUSTIVEIS.csv"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "logistics_01"
