CREATE TABLE IF NOT EXISTS rf_ext_anp.producao_biodiesel_m3 (
    id INT64 OPTIONS(description="identificador único do registro")
    data STRING OPTIONS(description="Data da producao"),
    grande_regiao STRING OPTIONS(description="Região do Brasil"),
    unidade_federacao STRING OPTIONS(description="Estado do Brasil"),
    produtor STRING OPTIONS(description="Nome do produtor"),
    produto STRING OPTIONS(description="Nome do produto"),
    producao STRING OPTIONS(description="Quantidade do produto em m³"),
    data_criacao TIMESTAMP OPTIONS(description="data da criação da camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na trusted")
) PARTITION BY DATE(data_ingestao_rf);