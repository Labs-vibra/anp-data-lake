CREATE TABLE IF NOT EXISTS td_ext_anp.cbios_2024 (
	codigo_agente_regulado STRING,
	cnpj STRING,
	razao_social STRING,
	somatorio_emissoes NUMERIC,
	participacao_mercado NUMERIC,
	meta_definitiva_2024 NUMERIC,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);