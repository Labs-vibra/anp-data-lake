MERGE td_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados AS target
USING (
    SELECT
        FARM_FINGERPRINT(
          CONCAT(
            CAST(cnpj AS STRING), '-', 
            CAST(razao_social AS STRING), '-', 
            CAST(numero_de_ordem AS STRING), '-', 
            CAST(tipo_de_instalacao AS STRING), '-', 
            CAST(cep AS STRING), '-', 
            CAST(endereco_da_matriz AS STRING), '-', 
            CAST(numero AS STRING), '-', 
            CAST(bairro AS STRING), '-', 
            CAST(complemento AS STRING), '-', 
            CAST(municipio AS STRING), '-', 
            CAST(uf AS STRING)
          )
        ) AS id,
        cnpj,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(razao_social, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS razao_social,
        numero_de_ordem,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(tipo_de_instalacao, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS tipo_de_instalacao,
        cep,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(endereco_da_matriz, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS endereco_da_matriz,
        numero,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(bairro, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS bairro,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(complemento, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS complemento,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(municipio, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS municipio,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS uf,

        -- capacidade_total: remove caracteres extras, troca vírgula por ponto, cast seguro para NUMERIC
        IFNULL(
          SAFE_CAST(
            NULLIF(
              REPLACE(
                REGEXP_REPLACE(CAST(capacidade_total AS STRING), r'[^0-9,.\-]', ''), -- só mantém dígitos, vírgula, ponto e sinal
                ',', '.'
              ),
              ''
            ) AS NUMERIC
          ),
        0) AS capacidade_total,

        -- participacao_porcentagem: mesma lógica
        IFNULL(
          SAFE_CAST(
            NULLIF(
              REPLACE(
                REGEXP_REPLACE(CAST(participacao_porcentagem AS STRING), r'[^0-9,.\-]', ''), 
                ',', '.'
              ),
              ''
            ) AS NUMERIC
          ),
        0) AS participacao_porcentagem,

        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(administrador, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS administrador,
        numero_autorizacao,

        -- data_publicacao: tenta formatos mais comuns, evita erro com SAFE.PARSE_DATE
        COALESCE(
          SAFE.PARSE_DATE('%Y-%m-%d', CAST(data_publicacao AS STRING)),
          SAFE.PARSE_DATE('%d/%m/%Y', CAST(data_publicacao AS STRING)),
          SAFE.PARSE_DATE('%Y/%m/%d', CAST(data_publicacao AS STRING)),
          SAFE.PARSE_DATE('%d-%m-%Y', CAST(data_publicacao AS STRING))
        ) AS data_publicacao,

        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(status_pmqc, NFD))), r'[^a-zA-Z0-9_\s\-.\' ]', '') AS status_pmqc,
        data_criacao
    FROM rw_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados
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


