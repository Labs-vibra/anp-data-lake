CREATE TABLE IF NOT EXISTS td_ext_anp.logistica_03 (
    periodo DATE,
    produto STRING,
    uf_origem STRING,
    uf_destino STRING,
    vendedor STRING,
    comprador STRING,
    qtd_produto_liquido NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);