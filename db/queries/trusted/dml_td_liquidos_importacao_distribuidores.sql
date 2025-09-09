MERGE td_ext_anp.liquidos_importacao_distribuidores AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, '-', mes, '-', distribuidor, '-', regiao, '-', uf, '-', codigo_produto, '-', nome_produto, '-', descricao_produto, '-', regiao_origem, '-', uf_origem)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        LOWER(REGEXP_REPLACE(NORMALIZE(distribuidor, NFD), r'[\u0300-\u036f]', '')) AS distribuidor,
        LOWER(regiao) AS regiao,
        LOWER(uf) AS uf,
        codigo_produto,
        LOWER(REGEXP_REPLACE(NORMALIZE(nome_produto, NFD), r'[\u0300-\u036f]', '')) AS nome_produto,
        LOWER(REGEXP_REPLACE(NORMALIZE(descricao_produto, NFD), r'[\u0300-\u036f]', '')) AS descricao_produto,
        LOWER(regiao_origem) AS regiao_origem,
        LOWER(uf_origem) AS uf_origem,
        SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_importacao_distribuidores
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_importacao_distribuidores
    )
) AS source
ON  target.data                = source.data
AND target.distribuidor        = source.distribuidor
AND target.regiao              = source.regiao
AND target.uf                  = source.uf
AND target.codigo_produto      = source.codigo_produto
AND target.nome_produto        = source.nome_produto
AND target.descricao_produto   = source.descricao_produto
AND target.regiao_origem       = source.regiao_origem
AND target.uf_origem           = source.uf_origem
WHEN MATCHED AND (
    target.quantidade_produto_mil_m3 IS DISTINCT FROM source.quantidade_produto_mil_m3
    OR target.data_criacao IS DISTINCT FROM source.data_criacao
) THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        distribuidor,
        regiao,
        uf,
        codigo_produto,
        nome_produto,
        descricao_produto,
        regiao_origem,
        uf_origem,
        quantidade_produto_mil_m3,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.distribuidor,
        source.regiao,
        source.uf,
        source.codigo_produto,
        source.nome_produto,
        source.descricao_produto,
        source.regiao_origem,
        source.uf_origem,
        source.quantidade_produto_mil_m3,
        source.data_criacao
    );
