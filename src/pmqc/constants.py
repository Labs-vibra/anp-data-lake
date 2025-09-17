import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/pmqc/extracao/"

URL_BASE = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "pmqc-programa-de-monitoramento-da-qualidade-dos-combustiveis"
)

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "pmqc"

COLUMNS = [
    "data_coleta",
    "id_numeric",
    "grupo_produto",
    "produto",
    "razao_social_posto",
    "cnpj_posto",
    "distribuidora",
    "endereco",
    "complemento",
    "bairro",
    "municipo",
    "latitude",
    "longitude",
    "uf",
    "regiao_politica",
    "ensaio",
    "resultado",
    "unidade_ensaio",
    "conforme"
]

