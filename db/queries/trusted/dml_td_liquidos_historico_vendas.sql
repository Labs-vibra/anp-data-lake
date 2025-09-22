MERGE td_ext_anp.liquidos_vendas_historico AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, '-', mes, '-', distribuidor, '-', codigo_produto, '-', nome_produto, '-', regiao_origem, '-', uf_origem, '-', regiao_destinatario, '-', uf_destino)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(distribuidor, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS distribuidor,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(codigo_produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS codigo_produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(nome_produto, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS nome_produto,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(regiao_origem, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS regiao_origem,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf_origem, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf_origem,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(regiao_destinatario, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS regiao_destinatario,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(uf_destino, NFD))), '[^a-zA-Z0-9_\\s-.\']', '') AS uf_destino,
        IFNULL(SAFE_CAST(NULLIF(REPLACE(quantidade_produto_mil_m3, ',', '.'), '') AS NUMERIC), 0) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_historico_vendas
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_historico_vendas
    )
) AS source
ON  target.data                = source.data
AND target.distribuidor        = source.distribuidor
AND target.codigo_produto      = source.codigo_produto
AND target.nome_produto        = source.nome_produto
AND target.regiao_origem       = source.regiao_origem
AND target.uf_origem           = source.uf_origem
AND target.regiao_destinatario = source.regiao_destinatario
AND target.uf_destino          = source.uf_destino
WHEN MATCHED AND (
    target.quantidade_produto_mil_m3 IS DISTINCT FROM source.quantidade_produto_mil_m3
) THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        distribuidor,
        codigo_produto,
        nome_produto,
        regiao_origem,
        uf_origem,
        regiao_destinatario,
        uf_destino,
        quantidade_produto_mil_m3,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.distribuidor,
        source.codigo_produto,
        source.nome_produto,
        source.regiao_origem,
        source.uf_origem,
        source.regiao_destinatario,
        source.uf_destino,
        source.quantidade_produto_mil_m3,
        source.data_criacao
    );
