import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#BIO_FOLDER = ""
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME_REGIAO = "producao_biodiesel_m3_regiao"

BASE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-biocombustiveis"