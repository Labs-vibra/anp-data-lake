MERGE td_ext_anp.contratos_cessao_espaco_carregamento AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(tipo_contrato, '-', razao_social_cedente, '-', cnpj_cedente, '-', numero_ao_da_cedente, '-', municipio_cedente, '-', uf_cedente, '-', razao_social_cessionaria, '-', cnpj_cessionaria, '-', numero_da_aea_cessionaria, '-', inicio_contrato_ato_homologacao, '-', processo, '-', termino_contrato)) AS id,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(tipo_contrato, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS tipo_contrato,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social_cedente, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social_cedente,
        cnpj_cedente,
        numero_ao_da_cedente,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(municipio_cedente, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS municipio_cedente,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf_cedente, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf_cedente,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social_cessionaria, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social_cessionaria,
        cnpj_cessionaria,
        numero_da_aea_cessionaria,
        inicio_contrato_ato_homologacao,
        processo,
        PARSE_DATE('%Y-%m-%d', cast(termino_contrato as STRING)) AS termino_contrato,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(CAST(volume_m3 AS STRING), ',', '.'), '') AS NUMERIC), 0) AS volume_m3,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(CAST(gasolina_a as STRING), ',', '.'), '') AS NUMERIC), 0) AS gasolina_a,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(CAST(gasolina_a_premium as STRING), ',', '.'), '') AS NUMERIC), 0) AS gasolina_a_premium,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(gasolina_c as string), ',', '.'), '') AS NUMERIC), 0) AS gasolina_c,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(b100 as string), ',', '.'), '') AS NUMERIC), 0) AS b100,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(eac as string), ',', '.'), '') AS NUMERIC), 0) AS eac,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(ehc as string), ',', '.'), '') AS NUMERIC), 0) AS ehc,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_diesel_a_s500 as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_a_s500,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_diesel_a_s10 as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_a_s10,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_diesel_b_s500 as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_b_s500,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_diesel_b_s10 as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_b_s10,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(diesel_c_s10 as string), ',', '.'), '') AS NUMERIC), 0) AS diesel_c_s10,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_diesel_mar as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_diesel_mar,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_combustivel_1a as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_combustivel_1a,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(oleo_combustivel_1b as string), ',', '.'), '') AS NUMERIC), 0) AS oleo_combustivel_1b,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(qav as string), ',', '.'), '') AS NUMERIC), 0) AS qav,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(querosene_iluminante as string), ',', '.'), '') AS NUMERIC), 0) AS querosene_iluminante,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(cast(gav as string), ',', '.'), '') AS NUMERIC), 0) AS gav,
        data_criacao
    FROM td_ext_anp.contratos_cessao_espaco_carregamento
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM td_ext_anp.contratos_cessao_espaco_carregamento
    )
) AS source
ON target.tipo_contrato = source.tipo_contrato
AND target.razao_social_cedente = source.razao_social_cedente
AND target.cnpj_cedente = source.cnpj_cedente
AND target.numero_ao_da_cedente = source.numero_ao_da_cedente
AND target.municipio_cedente = source.municipio_cedente
AND target.uf_cedente = source.uf_cedente
AND target.razao_social_cessionaria = source.razao_social_cessionaria
AND target.cnpj_cessionaria = source.cnpj_cessionaria
AND target.numero_da_aea_cessionaria = source.numero_da_aea_cessionaria
AND target.inicio_contrato_ato_homologacao = source.inicio_contrato_ato_homologacao
AND target.processo = source.processo
AND target.termino_contrato = source.termino_contrato
WHEN MATCHED THEN
    UPDATE SET
        volume_m3 = source.volume_m3,
        gasolina_a_premium = source.gasolina_a_premium,
        gasolina_c = source.gasolina_c,
        b100 = source.b100,
        eac = source.eac,
        ehc = source.ehc,
        oleo_diesel_a_s500 = source.oleo_diesel_a_s500,
        oleo_diesel_a_s10 = source.oleo_diesel_a_s10,
        oleo_diesel_b_s500 = source.oleo_diesel_b_s500,
        oleo_diesel_b_s10 = source.oleo_diesel_b_s10,
        diesel_c_s10 = source.diesel_c_s10,
        oleo_diesel_mar = source.oleo_diesel_mar,
        oleo_combustivel_1a = source.oleo_combustivel_1a,
        oleo_combustivel_1b = source.oleo_combustivel_1b,
        qav = source.qav,
        querosene_iluminante = source.querosene_iluminante,
        gav = source.gav,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
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
        volume_m3,
        gasolina_a,
        gasolina_a_premium,
        gasolina_c,
        b100,
        eac,
        ehc,
        oleo_diesel_a_s500,
        oleo_diesel_a_s10,
        oleo_diesel_b_s500,
        oleo_diesel_b_s10,
        diesel_c_s10,
        oleo_diesel_mar,
        oleo_combustivel_1a,
        oleo_combustivel_1b,
        qav,
        querosene_iluminante,
        gav,
        data_criacao
    )
    VALUES (
        source.id,
        source.tipo_contrato,
        source.razao_social_cedente,
        source.cnpj_cedente,
        source.numero_ao_da_cedente,
        source.municipio_cedente,
        source.uf_cedente,
        source.razao_social_cessionaria,
        source.cnpj_cessionaria,
        source.numero_da_aea_cessionaria,
        source.inicio_contrato_ato_homologacao,
        source.processo,
        source.termino_contrato,
        source.volume_m3,
        source.gasolina_a,
        source.gasolina_a_premium,
        source.gasolina_c,
        source.b100,
        source.eac,
        source.ehc,
        source.oleo_diesel_a_s500,
        source.oleo_diesel_a_s10,
        source.oleo_diesel_b_s500,
        source.oleo_diesel_b_s10,
        source.diesel_c_s10,
        source.oleo_diesel_mar,
        source.oleo_combustivel_1a,
        source.oleo_combustivel_1b,
        source.qav,
        source.querosene_iluminante,
        source.gav,
        source.data_criacao
    );