CREATE TABLE IF NOT EXISTS td_ext_anp.logistics_02 (
    periodo DATE,
    uf_destino STRING,
    produto STRING,
    vendedor STRING,
    qtd_produto_liquido NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
