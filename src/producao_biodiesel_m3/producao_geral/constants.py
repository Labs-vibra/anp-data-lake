import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#BIO_FOLDER = ""
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME_GERAL = "producao_biodiesel_m3_geral"

BASE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-biocombustiveis"

MAPPING_COLUM = {
    'ano': 'ano',
    'mes': 'mes',
    'grande_regiao': 'grande_regiao',
    'unidade_da_federacao': 'unidade_federacao',
    'produtor': 'produtor',
    'produto': 'produto',
    'producao': 'producao'
}