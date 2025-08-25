import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

CONGENERES_SALES_FILE_PATH = "anp/logistica/DADOS_ABERTOS_LOGISTICA_03_VENDAS_CONGNERES_DE_DISTRIBUIDORES.csv"

COLUMNS_MAPPING = {
    "Período": "periodo",
    "Produto": "produto",
    "UF Origem": "uf_origem",
    "UF Destino": "uf_destino",
    "Vendedor": "vendedor",
    "Comprador": "comprador",
    "Qtd  Produto Líquido": "qtd_produto_liquido",
}

LOGISTIC_03_TABLE_NAME = "rw_ext_anp.logistica_03"
