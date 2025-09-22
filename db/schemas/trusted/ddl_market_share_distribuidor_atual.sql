CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_entregas_distribuidor_atual (
    id INT64 OPTIONS(description="ID único do registro"),
    data DATE OPTIONS(description="Data da entrega"),
    distribuidor STRING OPTIONS(description="Nome do distribuidor"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
    regiao STRING OPTIONS(description="Sigla da região do Brasil"),
    quantidade_produto_mil_m3 NUMERIC OPTIONS(description="Quantidade do produto em mil m³"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão do registro na camada trusted")
) PARTITION BY DATE(data_ingestao_td);