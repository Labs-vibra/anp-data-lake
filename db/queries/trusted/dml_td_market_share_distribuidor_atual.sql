MERGE td_ext_anp.liquidos_entregas_distribuidor_atual AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, '-', mes, '-', distribuidor, '-', codigo_produto, '-', nome_produto, '-', regiao)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', LPAD(mes, 2, '0'), '-01')) AS data,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(distribuidor, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS distribuidor,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(codigo_produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS codigo_produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(nome_produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS nome_produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(regiao, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS regiao,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(quantidade_produto_mil_m3, ',', '.'), '') AS NUMERIC), 0) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_entregas_distribuidor_atual
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_entregas_distribuidor_atual
    )
) AS source
ON target.data = source.data
   AND target.distribuidor = source.distribuidor
   AND target.codigo_produto = source.codigo_produto
   AND target.regiao = source.regiao
   AND target.nome_produto = source.nome_produto
WHEN MATCHED AND (
    target.quantidade_produto_mil_m3 IS DISTINCT FROM source.quantidade_produto_mil_m3
)
THEN
    UPDATE SET
        target.quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3,
        target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id, `data`, distribuidor, codigo_produto, nome_produto, regiao, quantidade_produto_mil_m3, data_criacao
    )
    VALUES (
        source.id, source.data, source.distribuidor, source.codigo_produto, source.nome_produto, source.regiao, source.quantidade_produto_mil_m3, source.data_criacao
    );