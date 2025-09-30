MERGE td_ext_anp.postos_distribuidores AS target
USING (
	SELECT
		FARM_FINGERPRINT(CONCAT(cnpj_distribuidora, '-', razao_social_distribuidora, '-', delivery, '-', numero_despacho_delivery, '-', data_autorizacao_delivery, '-', codigo_instalacao_i_simp)) AS id,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(numero_autorizacao, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS numero_autorizacao,
		PARSE_DATE('%d/%m/%Y', LOWER(TRIM(NORMALIZE(data_publicacao_dou_autorizacao, NFD)))) AS data_publicacao_dou_autorizacao,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(codigo_instalacao_i_simp, NFD))), '[^0-9]', '') AS codigo_instalacao_i_simp,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(cnpj_distribuidora, NFD))), '[^0-9]', '') AS cnpj_distribuidora,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social_distribuidora, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social_distribuidora,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(cnpj_posto, NFD))), '[^0-9]', '') AS cnpj_posto,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social_posto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social_posto,
		data_criacao
