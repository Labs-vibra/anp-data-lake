MERGE td_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(nome_empresarial, '-', cod_instalacao, '-')) AS id,
        DATE(data) AS data,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(nome_empresarial, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS nome_empresarial,
        LOWER(TRIM(uf)) AS uf,
        LOWER(TRIM(municipio)) AS municipio,
        REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
        IFNULL(SAFE_CAST(NULLIF(cod_instalacao, '') AS NUMERIC), 0) AS cod_instalacao,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(segmento, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS segmento,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(detalhe_instalacao, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS detalhe_instalacao,
        REGEXP_REPLACE(TRIM(NORMALIZE(tag, NFD)), '[^a-zA-Z0-9_\\s-.\']', '') AS tag,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(tipo_da_unidade, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS tipo_da_unidade,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(grupo_de_produtos, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS grupo_de_produtos,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(tancagem_m3, ',', '.'), '') AS NUMERIC), 0) AS tancagem_m3,
        data_criacao
    FROM rw_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis
    )
) AS source
ON target.id                      = source.id
AND  target.data                  = source.data
AND target.nome_empresarial       = source.nome_empresarial
AND target.uf                     = source.uf
AND target.municipio              = source.municipio
AND target.cnpj                   = source.cnpj
AND target.cod_instalacao         = source.cod_instalacao
AND target.segmento               = source.segmento
AND target.detalhe_instalacao     = source.detalhe_instalacao
AND target.tag                    = source.tag
AND target.tipo_da_unidade        = source.tipo_da_unidade
AND target.grupo_de_produtos      = source.grupo_de_produtos
AND target.data_criacao           = source.data_criacao
WHEN MATCHED AND (
    target.tancagem_m3 IS DISTINCT FROM source.tancagem_m3
) THEN
    UPDATE SET
        tancagem_m3 = source.tancagem_m3,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        nome_empresarial,
        uf,
        municipio,
        cnpj,
        cod_instalacao,
        segmento,
        detalhe_instalacao,
        tag,
        tipo_da_unidade,
        grupo_de_produtos,
        tancagem_m3,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.nome_empresarial,
        source.uf,
        source.municipio,
        source.cnpj,
        source.cod_instalacao,
        source.segmento,
        source.detalhe_instalacao,
        source.tag,
        source.tipo_da_unidade,
        source.grupo_de_produtos,
        source.tancagem_m3,
        source.data_criacao
    );
