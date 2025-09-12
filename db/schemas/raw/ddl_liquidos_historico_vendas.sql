CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_historico_vendas (
    ano STRING OPTIONS(description="Ano da operação"),
    mes STRING OPTIONS(description="Mês da operação"),
    distribuidor STRING OPTIONS(description="Nome do distribuidor"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
    regiao_origem STRING OPTIONS(description="Sigla da região de origem"),
    uf_origem STRING OPTIONS(description="Sigla da unidade federativa de origem"),
    regiao_destinatario STRING OPTIONS(description="Sigla da região do destinatário"),
    uf_destino STRING OPTIONS(description="Sigla da unidade federativa de destino"),
    quantidade_produto_mil_m3 STRING OPTIONS(description="Quantidade do produto em mil m³"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na camada raw")
) PARTITION BY DATE(data_criacao);
