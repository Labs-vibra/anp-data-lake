CREATE TABLE IF NOT EXISTS td_ext_anp.liquidos_importacao_distribuidores (
    id INT64 OPTIONS(description="Identificador único do registro"),
    data DATE OPTIONS(description="Data da operação"),
    distribuidor STRING OPTIONS(description="Nome do distribuidor"),
    regiao STRING OPTIONS(description="Sigla da região do Brasil"),
    uf STRING OPTIONS(description="Unidade Federativa do Brasil"),
    codigo_produto STRING OPTIONS(description="Código do produto comercializado"),
    nome_produto STRING OPTIONS(description="Nome do produto comercializado"),
    descricao_produto STRING OPTIONS(description="Descrição do produto comercializado"),
    regiao_origem STRING OPTIONS(description="Sigla da região de origem do produto"),
    uf_origem STRING OPTIONS(description="Unidade Federativa de origem do produto"),
    quantidade_produto_mil_m3 NUMERIC OPTIONS(description="Quantidade do produto comercializado em mil m³"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão do registro na camada trusted")
) PARTITION BY DATE(data_ingestao_td);