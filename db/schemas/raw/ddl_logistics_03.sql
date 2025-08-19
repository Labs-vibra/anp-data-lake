CREATE TABLE IF NOT EXISTS rw_ext_anp.logistics_03 (
    periodo STRING,
    produto STRING,
    uf_origem STRING,
    uf_destino STRING,
    vendedor STRING,
    comprador STRING,
    qtd_produto_liquido STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
