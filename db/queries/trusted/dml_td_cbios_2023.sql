MERGE td_ext_anp.cbios_2023 AS target
USING (
SELECT
    razao_social,
	REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
	SAFE_CAST (meta_cnpe_2023_individualizada_cbios AS NUMERIC) AS meta_cnpe_2023_individualizada_cbios,
	SAFE_CAST (meta_individual_2022_nao_cumprida_cbios AS NUMERIC) AS meta_individual_2022_nao_cumprida_cbios,
	SAFE_CAST (meta_individual_2023_a_ser_cumprida_ate_20240331_cbios AS NUMERIC) AS meta_individual_2023_a_ser_cumprida_ate_20240331_cbios,
    data_criacao
FROM
    rw_ext_anp.cbios_2023
WHERE
    data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.cbios_2023)
) AS source
ON source.razao_social = target.razao_social
AND source.cnpj = target.cnpj
WHEN MATCHED THEN
UPDATE SET
    target.meta_cnpe_2023_individualizada_cbios = source.meta_cnpe_2023_individualizada_cbios,
    target.meta_individual_2022_nao_cumprida_cbios = source.meta_individual_2022_nao_cumprida_cbios,
    target.meta_individual_2023_a_ser_cumprida_ate_20240331_cbios = source.meta_individual_2023_a_ser_cumprida_ate_20240331_cbios,
    target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
INSERT (
    razao_social,
    cnpj,
    meta_cnpe_2023_individualizada_cbios,
    meta_individual_2022_nao_cumprida_cbios,
    meta_individual_2023_a_ser_cumprida_ate_20240331_cbios,
    data_criacao
)
VALUES (
    source.razao_social,
    source.cnpj,
    source.meta_cnpe_2023_individualizada_cbios,
    source.meta_individual_2022_nao_cumprida_cbios,
    source.meta_individual_2023_a_ser_cumprida_ate_20240331_cbios,
    source.data_criacao
);