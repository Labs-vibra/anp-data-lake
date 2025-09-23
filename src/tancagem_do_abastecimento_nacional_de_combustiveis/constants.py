import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/pmqc/extracao/"

URL_BASE = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/tancagem-do-abastecimento"
    "-nacional-de-combustiveis"
)

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "tancagem_do_abastecimento_nacional_de_combustiveis"

COLUMNS = [
    "data",
    "nome_empresarial",
    "uf",
    "municipio",
    "cnpj",
    "cod_instalacao",
    "segmento",
    "detalhe_instalacao",
    "tag",
    "tipo_da_unidade",
    "grupo_de_produtos",
    "tancagem_m3"
]
