import os

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#RAW Liquidos de vendas atual
liquidos_vendas_atual = "anp/market_share/extracao/20250825_LIQUIDOS_VENDAS_ATUAL.CSV"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "liquidos_vendas_atual"