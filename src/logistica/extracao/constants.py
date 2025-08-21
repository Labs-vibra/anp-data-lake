import os

BUCKET_NAME= os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

# ETL de Logística 1, 2 e 3:
LOGISTICS_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp/"
    "paineis-dinamicos-do-abastecimento/painel-dinamico-da-logistica-do-abastecimento-nacional-de-combustiveis"
)

LOGISTICS_ZIP_BUCKET_PATH = "anp/logistica/dados_logistica.zip"

LOGISTICS_EXTRACTION_BUCKET_PATH = "anp/logistica/"

LOGISTICS_CSV_FILENAME_KEYWORD = "LOGISTICA"

LOGISTICS_CSV_ALLOWED_NUMBERS = ("01", "02", "03")

LOGISTICS_CSV_EXTENSION = ".CSV"
