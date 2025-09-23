import os

# Configurações do BigQuery
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "distribuidores_exercicio_atividade"

# URL do arquivo CSV da ANP
ANP_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/dcl/planilha-aea-filiais.csv"
