CREATE TABLE IF NOT EXISTS rw_ext_anp.cbios_2019 (
	codigo_agente_regulado STRING,
	cnpj STRING,
	razao_social STRING,
	somatorio_emissoes STRING,
	participacao_mercado STRING,
	meta_individual_2019 STRING,
	meta_individual_2019_diaria STRING,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);