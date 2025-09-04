CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_vendas_atual (
    ano STRING,
    mes STRING,
    agente_regulado STRING,
    codigo_produto STRING,
    nome_produto STRING,
    descricao_produto STRING,
    regiao_origem STRING,
    uf_origem STRING,
    regiao_destinatario STRING,
    uf_destino STRING,
    mercado_destinatario STRING,
    quantidade_produto_mil_m3 STRING,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);
