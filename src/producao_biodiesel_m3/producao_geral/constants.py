import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#BIO_FOLDER = ""
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME_GERAL = "producao_biodiesel_m3_geral"
TABLE_NAME_REGIAO = "producao_biodiesel_m3_regiao"

MAPPING_COLUMNS_GERAL = {
    "ANO": "ano",
    "MÊS": "mes",
    "GRANDE REGIÃO": "grande_regiao",
    "UNIDADE DA FEDERAÇÃO": "unidade_da_federacao",
    "PRODUTOR": "produtor",
    "PRODUTO": "produto",
    "PRODUÇÃO": "producao",
}

COLUMNS_GERAL = {
    'ano',
    'mes',
    'grande_regiao',
    'unidade_da_federacao',
    'produtor',
    'produto',
    'producao'
}

MAPPING_COLUMNS_REGIAO = {
    "ANO": "ano",
    "MÊS": "mes",
    "GRANDE REGIÃO": "grande_regiao",
    "PRODUÇÃO": "producao",
}

COLUMNS_REGIAO = {
    'ano',
    'mes',
    'grande_regiao',
    'producao'
}

BASE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-biocombustiveis"