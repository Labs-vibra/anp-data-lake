CREATE TABLE IF NOT EXISTS rw_ext_anp.producao_biodiesel_m3_geral (
    ano STRING OPTIONS(description="Ano da producao"),
    mes STRING OPTIONS(description="Mês da producao"),
    grande_regiao STRING OPTIONS(description="Região do Brasil"),
    producao STRING OPTIONS(description="Quantidade do produto em m³"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro")
) PARTITION BY DATE(data_criacao);