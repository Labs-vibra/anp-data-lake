MERGE td_ext_anp.aposentadoria_cbios AS target
USING (
    SELECT
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

