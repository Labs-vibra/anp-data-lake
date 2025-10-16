import os

BASE_URL = (
    "https://cdp.anp.gov.br/ords/r/cdp_apex/consulta-dados-publicos-cdp/"
    "base-de-distribui%C3%A7%C3%A3o-e-trr-autorizados-lista"
)

SELECT_ID = "P25_QUALIFICACAO"
CONSULT_BUTTON_ID = "B479395808106517986"

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/consulta_bases_de_distribuicao_e_trr_autorizados/"

#PASTA_DOWNLOAD_RAM = "/dev/shm/anp_downloads"
PASTA_DOWNLOAD_RAM = "/tmp/anp_downloads"
if not os.path.exists(PASTA_DOWNLOAD_RAM):
    os.makedirs(PASTA_DOWNLOAD_RAM)
