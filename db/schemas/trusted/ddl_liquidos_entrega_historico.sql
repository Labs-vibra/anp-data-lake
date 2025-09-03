CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_entrega_historico (
    data DATE,
    fornecedor_destino STRING,
    distribuidor_origem STRING,
    codigo_produto STRING,
    nome_produto STRING,
    regiao_origem STRING,
    uf_origem STRING,
    localidade_destino STRING,
    regiao_destinatario STRING,
    uf_destino STRING,
    quantidade_produto_mil_m3 NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
