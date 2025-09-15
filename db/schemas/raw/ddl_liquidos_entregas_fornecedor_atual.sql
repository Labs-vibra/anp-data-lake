CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_entregas_fornecedor_atual (
    ano STRING OPTIONS(description="Ano"),
    mes STRING OPTIONS(description="Mês"),
    fornecedor STRING OPTIONS(description="Fornecedor"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
	regiao STRING OPTIONS(description="Região da distribuidora"),
    quantidade_produto_mil_m3 STRING OPTIONS(description="Volume do produto"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data da ingestão na camada Raw")
) PARTITION BY DATE(data_criacao)
OPTIONS (
  description = "Dados brutos da ANP - entregas de líquidos por fornecedor"
);