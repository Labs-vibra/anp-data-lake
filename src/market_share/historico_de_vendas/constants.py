import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("GOOGLE_BUCKET_NAME", "vibra-dtan-jur-anp-input")

MARKET_SHARE_FOLDER = "anp/market_share/extracao"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "liquidos_historico_vendas"

COLUMNS = [
    "ano",
    "mes",
    "distribuidor",
    "codigo_produto",
    "nome_produto",
    "regiao_origem",
    "uf_origem",
    "regiao_destinatario",
    "uf_destino",
    "quantidade_produto_mil_m3"
]
