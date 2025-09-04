import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

#RAW Liquidos de vendas atual
MARKET_SHARE_FOLDER = "anp/market_share/extracao/"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "liquidos_vendas_atual"

MAPPING_COLUMNS = {
    "Ano": "ano",
    "Mês": "mes",
    "Agente Regulado": "agente_regulado",
    "Código do Produto": "codigo_produto",
    "Nome do Produto": "nome_produto",
    "Descrição do Produto": "descricao_produto",
    "Região Origem": "regiao_origem",
    "UF Origem": "uf_origem",
    "Região Destinatário": "regiao_destinatario",
    "UF Destino": "uf_destino",
    "Mercado Destinatário": "mercado_destinatario",
    "Quantidade de Produto (mil m³)": "quantidade_produto_mil_m3"
}
