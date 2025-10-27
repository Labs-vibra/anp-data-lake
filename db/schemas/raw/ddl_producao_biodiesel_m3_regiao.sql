CREATE TABLE IF NOT EXISTS rw_ext_anp.producao_biodiesel_m3_regiao (
    ano STRING OPTIONS(description="Ano da producao"),
    mes STRING OPTIONS(description="Mês da producao"),
    grande_regiao STRING OPTIONS(description="Região do Brasil"),
    unidade_da_federacao STRING OPTIONS(description="Unidade de Federação"),
    produto STRING OPTIONS(description="Nome do produto"),
    producao STRING OPTIONS(description="Quantidade do produto em m³"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro")
) PARTITION BY DATE(data_criacao);
