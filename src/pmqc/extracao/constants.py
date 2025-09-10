import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

ZIP_URL = "hhttps://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/" \
"pmqc-programa-de-monitoramento-da-qualidade-dos-combustiveis"

BUCKET_PATH = f"anp/pmqc/extracao/"
