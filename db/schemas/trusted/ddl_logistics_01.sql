CREATE TABLE IF NOT EXISTS td_ext_anp.logistics_01 (
    periodo DATE,
    uf_origem STRING,
    uf_destino STRING,
    produto STRING,
    classificacao STRING,
    sub_classificacao STRING,
    operacao STRING,
    modal STRING,
    qtd_produto_liquido NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
