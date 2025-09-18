MERGE td_ext_anp.pmqc AS target
USING (
    SELECT
        FARM_FINGERPRINT(id_numeric) AS id,
        PARSE_DATE('%Y-%m-%d', data_coleta) AS data_coleta,
        id_numeric,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(grupo_produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS grupo_produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social_posto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social_posto,
        REGEXP_REPLACE(cnpj_posto, '[^0-9]', '') AS cnpj_posto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(distribuidora, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS distribuidora,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(endereco, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS endereco,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(complemento, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS complemento,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(bairro, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS bairro,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(municipio, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS municipio,
        SAFE_CAST(REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(latitude, NFD))), '[^-0-9.]', '') AS NUMERIC) AS latitude,
        SAFE_CAST(REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(longitude, NFD))), '[^-0-9.]', '') AS NUMERIC) AS longitude,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(regiao_politica, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS regiao_politica,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(ensaio, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS ensaio,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(resultado, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS resultado,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(unidade_ensaio, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS unidade_ensaio,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(conforme, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS conforme,
        CURRENT_TIMESTAMP() AS data_ingestao_td,
        data_criacao
    FROM
        rw_ext_anp.pmqc
    WHERE
        data_criacao = (SELECT MAX(data_criacao)
        FROM rw_ext_anp.pmqc
    )
) AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        data_coleta = source.data_coleta,
        id_numeric = source.id_numeric,
        grupo_produto = source.grupo_produto,
        produto = source.produto,
        razao_social_posto = source.razao_social_posto,
        cnpj_posto = source.cnpj_posto,
        distribuidora = source.distribuidora,
        endereco = source.endereco,
        complemento = source.complemento,
        bairro = source.bairro,
        municipio = source.municipio,
        latitude = source.latitude,
        longitude = source.longitude,
        uf = source.uf,
        regiao_politica = source.regiao_politica,
        ensaio = source.ensaio,
        resultado = source.resultado,
        unidade_ensaio = source.unidade_ensaio,
        conforme = source.conforme,
        data_ingestao_td = source.data_ingestao_td
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data_coleta,
        id_numeric,
        grupo_produto,
        produto,
        razao_social_posto,
        cnpj_posto,
        distribuidora,
        endereco,
        complemento,
        bairro,
        municipio,
        latitude,
        longitude,
        uf,
        regiao_politica,
        ensaio,
        resultado,
        unidade_ensaio,
        conforme,
        data_criacao,
        data_ingestao_td
    )
    VALUES (
        source.id,
        source.data_coleta,
        source.id_numeric,
        source.grupo_produto,
        source.produto,
        source.razao_social_posto,
        source.cnpj_posto,
        source.distribuidora,
        source.endereco,
        source.complemento,
        source.bairro,
        source.municipio,
        source.latitude,
        source.longitude,
        source.uf,
        source.regiao_politica,
        source.ensaio,
        source.resultado,
        source.unidade_ensaio,
        source.conforme,
        source.data_criacao,
        source.data_ingestao_td
    )