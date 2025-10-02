import os

FILES_TO_EXTRACT_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/mav/"
)

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

FILES_BUCKET_DESTINATION = "anp/multas_aplicadas_acoes_fiscalizacao"

SCRAPPING_HTML_XPATH = 'article.entry header span.summary a'