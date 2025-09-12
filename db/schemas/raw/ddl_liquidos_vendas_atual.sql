CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_vendas_atual (
    ano STRING OPTIONS (description="Ano da venda"),
    mes STRING OPTIONS (description="Mês da venda"),
    agente_regulado STRING OPTIONS (description="Nome do agente regulado"),
    codigo_produto STRING OPTIONS (description="Código do produto"),
    nome_produto STRING OPTIONS (description="Nome do produto"),
    descricao_produto STRING OPTIONS (description="Descrição do produto"),
    regiao_origem STRING OPTIONS (description="Sigla da região de origem"),
    uf_origem STRING OPTIONS (description="Unidade federativa de origem"),
    regiao_destinatario STRING OPTIONS (description="Sigla da região destinatária"),
    uf_destino STRING OPTIONS (description="Unidade federativa de destino"),
    mercado_destinatario STRING OPTIONS (description="Mercado destinatário"),
    quantidade_produto_mil_m3 STRING OPTIONS (description="Quantidade do produto vendido em mil m³"),
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Data de criação do registro na camada raw")
) PARTITION BY DATE(data_criacao);
