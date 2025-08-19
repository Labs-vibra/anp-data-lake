CREATE TABLE IF NOT EXISTS rw_ext_anp.cbios_2021 (
	codigo_agente_regulado STRING,
	cnpj STRING,
	razao_social STRING,
	somatorio_emissoes STRING,
	participacao_mercado STRING,
	meta_individual_2021 STRING,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);