CREATE TABLE IF NOT EXISTS td_ext_anp.cbios_2023 (
	razao_social STRING, 
	cnpj STRING, 
	meta_cnpe_2023_individualizada_cbios NUMERIC,
	meta_individual_2022_nao_cumprida_cbios NUMERIC,
	meta_individual_2023_a_ser_cumprida_ate_20240331_cbios NUMERIC,
	data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);