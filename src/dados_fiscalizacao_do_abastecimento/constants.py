import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/dados_fiscalizacao_do_abastecimento/extracao/"

URL_BASE = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp/"
    "painel-dinamico-da-fiscalizacao-do-abastecimento"
)

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "dados_fiscalizacao_do_abastecimento"

COLUMNS = [
    "uf",
    "municipio",
    "bairro",
    "endereco",
    "cnpj_ou_cpf",
    "agente_economico",
    "segmento_fiscalizado",
    "data_do_df",
    "numero_do_documento",
    "procedimento_de_fiscalizacao",
    "resultado"
]
