MERGE td_ext_anp.liquidos_entregas_fornecedor_atual AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, mes, fornecedor, codigo_produto, nome_produto, regiao)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        LOWER(REGEXP_REPLACE(NORMALIZE(fornecedor, NFD), r'\pM', '')) AS fornecedor,
        codigo_produto,
        LOWER(REGEXP_REPLACE(NORMALIZE(nome_produto, NFD), r'\pM', '')) AS nome_produto,
        LOWER(REGEXP_REPLACE(NORMALIZE(regiao, NFD), r'\pM', '')) AS regiao,
        IFNULL(SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC), 0) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_entregas_fornecedor_atual
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_entregas_fornecedor_atual
    )
) AS source
ON  target.data                = source.data
AND target.fornecedor          = source.fornecedor
AND target.codigo_produto      = source.codigo_produto
AND target.nome_produto        = source.nome_produto
AND target.regiao              = source.regiao
WHEN MATCHED THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3,
        target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        fornecedor,
        codigo_produto,
        nome_produto,
        regiao,
        quantidade_produto_mil_m3,
        data_criacao
    )
    VALUES (
        source.data,
        source.fornecedor,
        source.codigo_produto,
        source.nome_produto,
        source.regiao,
        source.quantidade_produto_mil_m3,
        source.data_criacao
    );
