CREATE TABLE IF NOT EXISTS td_ext_anp.producao_biodiesel_m3_geral (
    id INT64 OPTIONS(description="ID único do registro")
    data STRING OPTIONS(description="Data da producao"),
    grande_regiao STRING OPTIONS(description="Região do Brasil"),
    unidade_federacao STRING OPTIONS(description="Estado do Brasil"),
    produtor STRING OPTIONS(description="Nome do produtor"),
    produto STRING OPTIONS(description="Nome do produto"),
    producao STRING OPTIONS(description="Quantidade do produto em m³"),
    data_criacao TIMESTAMP OPTIONS(description="Data de inserção dos dados na camada raw")
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de inserção dos dados na camada trusted")
) PARTITION BY DATE(data_ingestao_td);