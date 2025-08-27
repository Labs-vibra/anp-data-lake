import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("GOOGLE_BUCKET_NAME", "vibra-dtan-jur-anp-input")

MARKET_SHARE_FOLDER = "anp/market_share/extracao"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
BQ_DATASET = "rw_ext_anp" 
TABLE_NAME = "liquidos_entregas_fornecedor_atual"

MAPPING_COLUMNS = {
		'Ano': 'ano',
		'Mês': 'mes',
		'Fornecedor': 'fornecedor',
		'Código do Produto': 'codigo_produto',
		'Nome do Produto': 'nome_produto',
		'Região': 'regiao',
        'Quantidade de Produto (mil m³)': 'quantidade_produto_mil_m3',
}
