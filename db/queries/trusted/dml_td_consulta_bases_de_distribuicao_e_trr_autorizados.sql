MERGE td_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(cnpj, '-', razao_social, '-', numero_de_ordem, '-', tipo_de_instalacao, '-', cep, '-', endereco_da_matriz, '-', numero, '-', bairro, '-', complemento, '-', municipio, '-', uf)) AS id,
        cnpj,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS razao_social,
        numero_de_ordem,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(tipo_de_instalacao, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS tipo_de_instalacao,
        cep,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(endereco_da_matriz, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS endereco_da_matriz,
        numero,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(bairro, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS bairro,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(complemento, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS complemento,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(municipio, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS municipio,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(capacidade_total, ',', '.'), '') AS NUMERIC), 0) AS capacidade_total,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(participacao_porcentagem, ',', '.'), '') AS NUMERIC), 0) AS participacao_porcentagem,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(administrador, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS administrador,
        numero_autorizacao,
        PARSE_DATE('%Y-%m-%d', data_publicacao) AS data_publicacao,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(status_pmqc, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS status_pmqc,
        data_criacao
    FROM td_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM td_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados
    )
) AS source
ON target.cnpj = source.cnpj
AND target.razao_social = source.razao_social
AND target.numero_de_ordem = source.numero_de_ordem
AND target.tipo_de_instalacao = source.tipo_de_instalacao
AND target.cep = source.cep
AND target.endereco_da_matriz = source.endereco_da_matriz
AND target.numero = source.numero
AND target.bairro = source.bairro
AND target.complemento = source.complemento
AND target.municipio = source.municipio
AND target.uf = source.uf
AND target.numero_autorizacao = source.numero_autorizacao
AND target.data_publicacao = source.data_publicacao
WHEN MATCHED THEN
    UPDATE SET
        capacidade_total = source.capacidade_total,
        participacao_porcentagem = source.participacao_porcentagem,
        administrador = source.administrador,
        status_pmqc = source.status_pmqc,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        cnpj,
        razao_social,
        numero_de_ordem,
        tipo_de_instalacao,
        cep,
        endereco_da_matriz,
        numero,
        bairro,
        complemento,
        municipio,
        uf,
        capacidade_total,
        participacao_porcentagem,
        administrador,
        numero_autorizacao,
        data_publicacao,
        status_pmqc,
        data_criacao
    )
    VALUES (
        source.id,
        source.cnpj,
        source.razao_social,
        source.numero_de_ordem,
        source.tipo_de_instalacao,
        source.cep,
        source.endereco_da_matriz,
        source.numero,
        source.bairro,
        source.complemento,
        source.municipio,
        source.uf,
        source.capacidade_total,
        source.participacao_porcentagem,
        source.administrador,
        source.numero_autorizacao,
        source.data_publicacao,
        source.status_pmqc,
        source.data_criacao
    );

