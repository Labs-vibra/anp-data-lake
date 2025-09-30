import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/consulta_bases_de_distribuicao_e_trr_autorizados"

URL_BASE = ("https://cdp.anp.gov.br/ords/r/cdp_apex/consulta-dados-publicos-cdp"
            "/base-de-distribui%C3%A7%C3%A3o-e-trr-autorizados-lista?clear")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "tancagem_do_abastecimento_nacional_de_combustiveis"

COLUMNS = [
]
