MERGE td_ext_anp.aposentadoria_cbios AS target
USING (
    tipo_contrato,
    razao_social_cedente,
    cnpj_cedente,
    numero_ao_da_cedente,
    municipio_cedente,
    uf_cedente,
    razao_social_cessionaria,
    cnpj_cessionaria,
    numero_da_aea_cessionaria,
    inicio_contrato_ato_homologacao,
    processo,
    termino_contrato,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(volume_m3, ',', '.'), '') AS NUMERIC), 0) AS volume_m3,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(gasolina_a, ',', '.'), '') AS NUMERIC), 0) AS gasolina_a,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(gasolina_a_premium, ',', '.'), '') AS NUMERIC), 0) AS gasolina_a_premium,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(gasolina_c, ',', '.'), '') AS NUMERIC), 0) AS gasolina_c,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(b100, ',', '.'), '') AS NUMERIC), 0) AS b100,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(eac, ',', '.'), '') AS NUMERIC), 0) AS eac,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(ehc, ',', '.'), '') AS NUMERIC), 0) AS ehc,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_diesel_a_s500, ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_a_s500,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_diesel_a_s10, ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_a_s10,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_diesel_b_s500, ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_b_s500,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_diesel_b_s10, ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_b_s10,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(diesel_c_s10, ',', '.'), '') AS NUMERIC), 0) AS diesel_c_s10,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_diesel_mar, ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_mar,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_combustivel_1a, ',', '.'), '') AS NUMERIC), 0) AS oleo_combustivel_1a,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(oleo_combustivel_1b, ',', '.'), '') AS NUMERIC), 0) AS oleo_combustivel_1b,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(qav, ',', '.'), '') AS NUMERIC), 0) AS qav,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(querosene_iluminante, ',', '.'), '') AS NUMERIC), 0) AS querosene_iluminante,
    IFNULL(SAFE_CAST(NULLIF(REPLACE(gav, ',', '.'), '') AS NUMERIC), 0) AS gav,
        FARM_FINGERPRINT(CONCAT(
        data, '-',
        CAST(data_criacao AS STRING), '-',
        CAST(CURRENT_TIMESTAMP AS STRING))) AS id,
        PARSE_DATE('%d/%m/%Y', data) AS data,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(quantidade_parte_obrigada, ',', '.'), '') AS NUMERIC), 0) AS quantidade_parte_obrigada,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(quantidade_parte_nao_obrigada, ',', '.'), '') AS NUMERIC), 0) AS quantidade_parte_nao_obrigada,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(totalizacao, ',', '.'), '') AS NUMERIC), 0) AS totalizacao,
        data_criacao,
        CURRENT_TIMESTAMP() AS data_ingestao_td
    FROM rw_ext_anp.aposentadoria_cbios
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.aposentadoria_cbios
    )
) AS source
ON source.data = target.data
WHEN MATCHED AND (
    source.quantidade_parte_obrigada IS DISTINCT FROM target.quantidade_parte_obrigada
    OR source.quantidade_parte_nao_obrigada IS DISTINCT FROM target.quantidade_parte_nao_obrigada
    OR source.totalizacao IS DISTINCT FROM target.totalizacao
) THEN
    UPDATE SET
        id = source.id,
        quantidade_parte_obrigada = source.quantidade_parte_obrigada,
        quantidade_parte_nao_obrigada = source.quantidade_parte_nao_obrigada,
        totalizacao = source.totalizacao,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        quantidade_parte_obrigada,
        quantidade_parte_nao_obrigada,
        totalizacao,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.quantidade_parte_obrigada,
        source.quantidade_parte_nao_obrigada,
        source.totalizacao,
        source.data_criacao
    );

