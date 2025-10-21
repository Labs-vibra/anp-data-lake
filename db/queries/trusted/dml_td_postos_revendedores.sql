MERGE td_ext_anp.postos_revendedores AS target
USING (
	SELECT
		FARM_FINGERPRINT(CONCAT(cnpj, '-', razao_social, '-', delivery, '-', numero_despacho_delivery, '-', data_autorizacao_delivery, '-', codigo_instalacao_i_simp)) AS id,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(numero_autorizacao, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS numero_autorizacao,
		SAFE.PARSE_DATE('%d/%m/%Y', data_publicacao_dou_autorizacao) AS data_publicacao_dou_autorizacao,
		TRIM(codigo_instalacao_i_simp) AS codigo_instalacao_i_simp,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(cnpj, NFD))), '[^0-9]', '') AS cnpj,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(endereco, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS endereco,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(complemento, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS complemento,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(bairro, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS bairro,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(cep, NFD))), '[^0-9]', '') AS cep,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(municipio, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS municipio,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(vinculacao_a_distribuidor, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS vinculacao_a_distribuidor,
		SAFE.PARSE_DATE('%d/%m/%Y', data_de_vinculacao_a_distribuidor) AS data_de_vinculacao_a_distribuidor,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS produto,
		IFNULL(SAFE_CAST(NULLIF(tancagem_m3, '') AS NUMERIC), 0) AS tancagem_m3,
		IFNULL(SAFE_CAST(NULLIF(qtde_de_bico, '') AS NUMERIC), 0) AS qtde_de_bico,
		TRIM(latitude) AS latitude,
		TRIM(longitude) AS longitude,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(delivery, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS delivery,
		SAFE.PARSE_DATE('%d/%m/%Y', data_autorizacao_delivery) AS data_autorizacao_delivery,
		LOWER(TRIM(NORMALIZE(numero_despacho_delivery, NFD))) AS numero_despacho_delivery,
		REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(status_pmqc, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS status_pmqc,
		data_criacao
	FROM
		rw_ext_anp.postos_revendedores
	WHERE
		data_criacao = (SELECT MAX(data_criacao)
		FROM rw_ext_anp.postos_revendedores
	)
) AS source
ON target.id = source.id
WHEN MATCHED THEN
	UPDATE SET
		tancagem_m3 = source.tancagem_m3,
		qtde_de_bico = source.qtde_de_bico,
		data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
	INSERT (
		id,
		numero_autorizacao,
		data_publicacao_dou_autorizacao,
		codigo_instalacao_i_simp,
		razao_social,
		cnpj,
		endereco,
		complemento,
		bairro,
		cep,
		uf,
		municipio,
		vinculacao_a_distribuidor,
		data_de_vinculacao_a_distribuidor,
		produto,
		tancagem_m3,
		qtde_de_bico,
		latitude,
		longitude,
		delivery,
		data_autorizacao_delivery,
		numero_despacho_delivery,
		status_pmqc,
		data_criacao
	)
	VALUES (
		source.id,
		source.numero_autorizacao,
		source.data_publicacao_dou_autorizacao,
		source.codigo_instalacao_i_simp,
		source.razao_social,
		source.cnpj,
		source.endereco,
		source.complemento,
		source.bairro,
		source.cep,
		source.uf,
		source.municipio,
		source.vinculacao_a_distribuidor,
		source.data_de_vinculacao_a_distribuidor,
		source.produto,
		source.tancagem_m3,
		source.qtde_de_bico,
		source.latitude,
		source.longitude,
		source.delivery,
		source.data_autorizacao_delivery,
		source.numero_despacho_delivery,
		source.status_pmqc,
		source.data_criacao
	);
