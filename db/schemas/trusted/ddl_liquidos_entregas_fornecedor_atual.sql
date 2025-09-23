CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_entregas_fornecedor_atual (
    id INT64 OPTIONS(description="Identificador único gerado via FARM_FINGERPRINT"),
    data DATE OPTIONS(description="Data"),
    fornecedor STRING OPTIONS(description="Fornecedor"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
	regiao STRING OPTIONS(description="Região da distribuidora"),
    quantidade_produto_mil_m3 NUMERIC OPTIONS(description="Volume do produto"),
    data_criacao TIMESTAMP OPTIONS(description="Data da ingestão na camada Raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data da ingestão na camada Trusted")
) PARTITION BY DATE(data_ingestao_td)
OPTIONS (
  description = "Dados tratados da ANP - entregas de líquidos por fornecedor"
);