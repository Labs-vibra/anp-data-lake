CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_vendas_atual (
    id INT64 OPTIONS (description="Identificador único da linha"),
    data DATE OPTIONS (description="Data no formato AAAA-MM-01, representando o mês e ano da venda"),
    agente_regulado STRING OPTIONS (description="Nome do agente regulado"),
    codigo_produto STRING OPTIONS (description="Código do produto"),
    nome_produto STRING OPTIONS (description="Nome do produto"),
    descricao_produto STRING OPTIONS (description="Descrição do produto"),
    regiao_origem STRING OPTIONS (description="Sigla da região de origem"),
    uf_origem STRING OPTIONS (description="Unidade federativa de origem"),
    regiao_destinatario STRING OPTIONS (description="Sigla da região destinatária"),
    uf_destino STRING OPTIONS (description="Unidade federativa de destino"),
    mercado_destinatario STRING OPTIONS (description="Mercado destinatário"),
    quantidade_produto_mil_m3 NUMERIC OPTIONS (description="Quantidade do produto em mil m³"),
	data_criacao TIMESTAMP OPTIONS (description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Data de ingestão do registro na camada trusted")
) PARTITION BY DATE(data_ingestao_td);
