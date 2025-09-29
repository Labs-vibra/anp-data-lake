MERGE td_ext_anp.vendas_combustiveis_segment AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, '-', mes, '-', unidade_da_federacao, '-', produto, '-', segmento)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(unidade_da_federacao, NFD))), [^a-zA-Z0-9_\\s-.\']) AS unidade_da_federacao,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(produto, NFD))), [^a-zA-Z0-9_\\s-.\']) AS produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(segmento, NFD))), [^a-zA-Z0-9_\\s-.\']) AS segmento,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(vendas , ',', '.') , '') AS NUMERIC), 0) AS vendas,
        data_criacao
    FROM
        rw_ext_anp.vendas_combustiveis_segmento
    WHERE
        data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.vendas_combustiveis_segmento)
) AS source
    ON source.data = target.data
   AND source.unidade_da_federacao = target.unidade_da_federacao
   AND source.produto = target.produto
   AND source.segmento = target.segmento
    WHEN MATCHED THEN
        UPDATE SET
        vendas = source.vendas,
        data_criacao = source.data_criacao
    WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        unidade_da_federacao,
        produto,
        segmento,
        vendas,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.unidade_da_federacao,
        source.produto,
        source.segmento,
        source.vendas,
        source.data_criacao
);