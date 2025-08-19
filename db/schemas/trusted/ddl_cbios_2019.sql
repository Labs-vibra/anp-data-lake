CREATE TABLE IF NOT EXISTS td_ext_anp.cbios_2019 (
	codigo_agente_regulado STRING,
	cnpj STRING,
	razao_social STRING,
	somatorio_emissoes NUMERIC,
	participacao_mercado NUMERIC,
	meta_individual_2019 NUMERIC,
	meta_individual_2019_diaria NUMERIC,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);