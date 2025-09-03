CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_entregas_distribuidor_atual (
    data DATE,
    distribuidor STRING,
    codigo_produto STRING,
    nome_produto STRING,
    regiao STRING,
    quantidade_produto_mil_m3 NUMERIC,
    data_criacao TIMESTAMP
) PARTITION BY DATE(data_criacao);