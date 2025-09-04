CREATE TABLE IF NOT EXISTS rw_ext_anp.aposentadoria_cbios (
    data STRING,
    quantidade_parte_obrigada STRING,
    quantidade_parte_nao_obrigada STRING,
    totalizacao STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);