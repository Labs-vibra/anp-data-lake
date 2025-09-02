from datetime import datetime
import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = os.getenv("BQ_DATASET", "rw_ext_anp")
TABLE_NAME = "liquidos_entregas_distribuidor_atual"

file_folder= "anp/market_share/extracao"

DISTRIBUTOR_COLUMN_MAPPING = {
    "Ano": "ano",
    "Mês": "mes",
    "Distribuidor": "distribuidor",
    "Código do Produto": "codigo_produto",
    "Nome do Produto": "nome_produto",
    "Região": "regiao",
    "Quantidade de Produto (mil m³)": "quantidade_produto_mil_m3",
}