CREATE TABLE IF NOT EXISTS rw_ext_anp.liquidos_entregas_distribuidor_atual (
    ano STRING OPTIONS(description="Ano da entrega"),
    mes STRING OPTIONS(description="Mês da entrega"),
    distribuidor STRING OPTIONS(description="Nome do distribuidor"),
    codigo_produto STRING OPTIONS(description="Código do produto"),
    nome_produto STRING OPTIONS(description="Nome do produto"),
    regiao STRING OPTIONS(description="Sigla da região do Brasil"),
    quantidade_produto_mil_m3 STRING OPTIONS(description="Quantidade do produto em mil m³"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro")
) PARTITION BY DATE(data_criacao);
