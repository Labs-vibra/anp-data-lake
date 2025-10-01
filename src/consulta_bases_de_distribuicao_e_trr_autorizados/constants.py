import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

BASE_URL = (
    "https://cdp.anp.gov.br/ords/r/cdp_apex/consulta-dados-publicos-cdp/"
    "base-de-distribui%C3%A7%C3%A3o-e-trr-autorizados-lista"
)

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "consulta_bases_de_distribuicao_e_trr_autorizados"

SELECT_ID = "P25_QUALIFICACAO"

# COLUMNS = [
#     "cnpj",
#     "razao_social",
#     "numero_de_ordem",
#     "tipo_de_instalacao",
#     "cep",
#     "endereco_da_matriz",
#     "numero",
#     "bairro",
#     "complemento",
#     "municipio",
#     "uf",
#     "capacidade_total",
#     "participacao_porcentagem",
#     "administrador",
#     "numero_autorizacao",
#     "data_publicacao",
#     "status_pmqc"
# ]
