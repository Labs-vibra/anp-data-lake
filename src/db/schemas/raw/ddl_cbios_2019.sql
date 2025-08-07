CREATE TABLE IF NOT EXISTS rw_ext_anp.cbios_2019 (
	codigo_agente_regulado STRING,
	cnpj STRING,
	razao_social STRING,
	somatorio_emissoes FLOAT64,
	participacao_mercado FLOAT64,
	meta_individual_2019 FLOAT64,
	meta_individual_2019_diaria FLOAT64
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);