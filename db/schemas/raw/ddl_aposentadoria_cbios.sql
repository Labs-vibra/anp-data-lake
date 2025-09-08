CREATE TABLE IF NOT EXISTS rw_ext_anp.aposentadoria_cbios (
    data STRING OPTIONS(description="Data de referência da aposentadoria"),
    quantidade_parte_obrigada STRING OPTIONS(description="Quantidade de CBIOs da parte obrigatória"),
    quantidade_parte_nao_obrigada STRING OPTIONS(description="Quantidade de CBIOs da parte não obrigatória"),
    totalizacao STRING OPTIONS(description="Totalização dos CBIOs"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro")
) PARTITION BY DATE(data_criacao);