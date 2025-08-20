MERGE td_ext_anp.cbios_2025 AS target
USING (
SELECT
	REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
	codigo_agente_regulado,
	REGEXP_REPLACE(cnpj_1, r'[^0-9]', '') AS cnpj_1,
	razao_social,
	SAFE_CAST(meta_cnpe_2025_individualizada_cbio AS NUMERIC) AS meta_cnpe_2025_individualizada_cbio,
	SAFE_CAST(cbios_a_abater_2024 AS NUMERIC) AS cbios_a_abater_2024,
	SAFE_CAST(meta_individual_nao_cumprida_2024 AS NUMERIC) AS meta_individual_nao_cumprida_2024,
	SAFE_CAST(meta_individual_2025_a_ser_cumprida_2025 AS NUMERIC) AS meta_individual_2025_a_ser_cumprida_2025,
	data_criacao
FROM
	rw_ext_anp.cbios_2025
WHERE
	data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.cbios_2025)
) AS source
ON source.codigo_agente_regulado = target.codigo_agente_regulado
AND source.cnpj = target.cnpj
AND source.razao_social = target.razao_social
AND source.cnpj_1 = target.cnpj_1
WHEN MATCHED THEN
UPDATE SET
	target.meta_cnpe_2025_individualizada_cbio = source.meta_cnpe_2025_individualizada_cbio,
	target.cbios_a_abater_2024 = source.cbios_a_abater_2024,
	target.meta_individual_nao_cumprida_2024 = source.meta_individual_nao_cumprida_2024,
	target.meta_individual_2025_a_ser_cumprida_2025 = source.meta_individual_2025_a_ser_cumprida_2025,
	target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
INSERT (
	codigo_agente_regulado,
	cnpj,
	razao_social,
	cnpj_1,
	meta_cnpe_2025_individualizada_cbio,
	cbios_a_abater_2024,
	meta_individual_nao_cumprida_2024,
	meta_individual_2025_a_ser_cumprida_2025,
	data_criacao
)
VALUES (
	source.codigo_agente_regulado,
	source.cnpj,
	source.razao_social,
	source.cnpj_1,
	source.meta_cnpe_2025_individualizada_cbio,
	source.cbios_a_abater_2024,
	source.meta_individual_nao_cumprida_2024,
	source.meta_individual_2025_a_ser_cumprida_2025,
	source.data_criacao
);