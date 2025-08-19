CREATE TABLE IF NOT EXISTS rw_ext_anp.logistica_02 (
    periodo STRING,
    uf_destino STRING,
    produto STRING,
    vendedor STRING,
    quantidade_produto_liquido STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
