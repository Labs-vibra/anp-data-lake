import os

BUCKET_NAME="ext-ecole-biomassa"

#RAW Logística 1
FILE_PATH = "extractions/DADOS ABERTOS - LOGISTICA 01 - ABASTECIMENTO NACIONAL DE COMBUST╓VEIS.csv"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "logistica_01"
