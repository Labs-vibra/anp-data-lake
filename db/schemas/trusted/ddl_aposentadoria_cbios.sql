CREATE TABLE IF NOT EXISTS td_ext_anp.aposentadoria_cbios (
	`data` DATE OPTIONS(description="Data de referência da aposentadoria"),
    quantidade_parte_obrigada NUMERIC OPTIONS(description="Quantidade de CBIOs da parte obrigatória"),
    quantidade_parte_nao_obrigada NUMERIC OPTIONS(description="Quantidade de CBIOs da parte não obrigatória"),
    totalizacao NUMERIC OPTIONS(description="Totalização dos CBIOs"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão na camada trusted")
) PARTITION BY DATE (data_ingestao_td);
