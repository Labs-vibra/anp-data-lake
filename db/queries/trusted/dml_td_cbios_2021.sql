MERGE td_ext_anp.cbios_2021 AS target
USING (
SELECT
    codigo_agente_regulado,
    REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
    razao_social,
    SAFE_CAST(somatorio_emissoes AS NUMERIC) AS somatorio_emissoes,
    SAFE_CAST(participacao_mercado AS NUMERIC) AS participacao_mercado,
    SAFE_CAST(meta_individual_2021 AS NUMERIC) AS meta_individual_2021,
    data_criacao
FROM
    rw_ext_anp.cbios_2021
WHERE
    data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.cbios_2021)
) AS source
ON source.codigo_agente_regulado = target.codigo_agente_regulado
AND source.cnpj = target.cnpj
AND source.razao_social = target.razao_social
WHEN MATCHED THEN
UPDATE SET
    target.somatorio_emissoes = source.somatorio_emissoes,
    target.participacao_mercado = source.participacao_mercado,
    target.meta_individual_2021 = source.meta_individual_2021,
    target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
INSERT (
    codigo_agente_regulado,
    cnpj,
    razao_social,
    somatorio_emissoes,
    participacao_mercado,
    meta_individual_2021,
    data_criacao
)
VALUES (
    source.codigo_agente_regulado,
    source.cnpj,
    source.razao_social,
    source.somatorio_emissoes,
    source.participacao_mercado,
    source.meta_individual_2021,
    source.data_criacao
);
