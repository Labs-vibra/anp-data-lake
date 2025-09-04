CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_importacao_distribuidores (
    data DATE,
    distribuidor STRING,
    regiao STRING,
    uf STRING,
    codigo_produto STRING,
    nome_produto STRING,
    descricao_produto STRING,
    regiao_origem STRING,
    uf_origem STRING,
    quantidade_produto_mil_m3 NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);