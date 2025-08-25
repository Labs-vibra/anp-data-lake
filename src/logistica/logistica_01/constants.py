import os

BUCKET_NAME= os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

#RAW Logística 1
LOGISTICS_01_FILE = "anp/logistica/DADOS_ABERTOS_LOGISTICA_01_ABASTECIMENTO_NACIONAL_DE_COMBUSTIVEIS.csv"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "logistica_01"
