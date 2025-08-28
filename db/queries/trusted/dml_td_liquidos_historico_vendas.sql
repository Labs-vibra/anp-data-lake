MERGE td_ext_anp.liquidos_vendas_historico AS target
USING (
    SELECT
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        distribuidor,
        codigo_produto,
        nome_produto,
        regiao_origem,
        uf_origem,
        regiao_destinatario,
        uf_destino,
        SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC) AS quantidade_produto_mil_m3,
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
WHEN MATCHED THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3
WHEN NOT MATCHED THEN
    INSERT (
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
