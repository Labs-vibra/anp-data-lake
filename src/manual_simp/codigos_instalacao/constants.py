import os

BUCKET_NAME= os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

CODIGOS_INSTALACAO_FILE = "anp/simp/T008Codigos_de_Instalacao.xlsx"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "codigos_instalacao"