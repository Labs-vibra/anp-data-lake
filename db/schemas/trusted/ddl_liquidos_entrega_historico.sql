CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_entrega_historico (
    id INT64 OPTIONS(description="Identificador único do registro"),
    data DATE OPTIONS(description="Data da operação"),
    fornecedor_destino STRING OPTIONS(description="Nome do fornecedor destinatário"),
    distribuidor_origem STRING OPTIONS(description="Nome do distribuidor de origem"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
    regiao_origem STRING OPTIONS(description="Sigla da região de origem"),
    uf_origem STRING OPTIONS(description="Unidade federativa de origem"),
    localidade_destino STRING OPTIONS(description="Localidade do destinatário"),
    regiao_destinatario STRING OPTIONS(description="Sigla da região do destinatário"),
    uf_destino STRING OPTIONS(description="Unidade federativa de destino"),
    quantidade_produto_mil_m3 NUMERIC OPTIONS(description="Quantidade do produto entregue em mil m³"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão do registro na camada trusted")
) PARTITION BY DATE(data_ingestao_td);
