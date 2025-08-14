import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "ext-ecole-biomassa")

MARKET_SALES_FILE_PATH = f"anp/logistica/DADOS_ABERTOS_LOGISTICA_02_VENDAS_NO_MERCADO_BRASILEIRO_DE_COMBUSTIVEIS.csv"

COLUMNS_MAPPING = {
    "Período": "periodo",
    "UF Destino": "uf_destino",
    "Produto": "produto",
    "Vendedor": "vendedor",
    "Qtd  Produto Líquido": "quantidade_produto_liquido",
    "Produto Líquido": "produto_liquido",
}

LOGISTIC_02_TABLE_NAME = "rw_ext_anp.logistica_02"