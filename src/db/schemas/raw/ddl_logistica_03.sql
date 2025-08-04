# CREATE TABLE IF NOT EXISTS rw_ext_anp.logistica_03 (
    periodo DATE,
    produto STRING,
    uf_origem STRING,
    uf_destino STRING,
    vendedor STRING,
    comprador STRING,
    qtd_produto_liquido FLOAT64,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
