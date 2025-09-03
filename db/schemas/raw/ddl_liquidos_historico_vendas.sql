CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_historico_vendas (
    ano STRING,
    mes STRING,
    distribuidor STRING,
    codigo_produto STRING,
    nome_produto STRING,
    regiao_origem STRING,
    uf_origem STRING,
    regiao_destinatario STRING,
    uf_destino STRING,
    quantidade_produto_mil_m3 STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
