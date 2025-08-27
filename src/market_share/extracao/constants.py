import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

ZIP_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp" \
    "/paineis-dinamicos-do-abastecimento/painel-dinamico-dados-liquidos.zip"

BUCKET_PATH = f"anp/market_share/extracao/"
