CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_vendas_atual (
    data DATE,
    agente_regulado STRING,
    codigo_produto STRING,
    nome_produto STRING,
    descricao_produto STRING,
    regiao_origem STRING,
    uf_origem STRING,
    regiao_destinatario STRING,
    uf_destino STRING,
    mercado_destinatario STRING,
    quantidade_produto_mil_m3 NUMERIC,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
