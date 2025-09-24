import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#RAW Liquidos de vendas atual
MARKET_SHARE_FOLDER = "anp/market_share/extracao/"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "producao_biodiesel_m3"

MAPPING_COLUMNS = {
    "ANO": "ano",
    "MÊS": "mes",
    "GRANDE REGIÃO": "grande_regiao",
    "UNIDADE DA FEDERAÇÃO": "codigo_produto",
    "PRODUTOR": "produtor",
    "PRODUTO": "produto",
    "PRODUÇÃO": "producao",
}

MAPPING_COLUMNS_REGIAO = {
    "ANO": "ano",
    "MÊS": "mes",
    "GRANDE REGIÃO": "grande_regiao",
    "PRODUÇÃO": "producao",
}