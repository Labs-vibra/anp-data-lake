MERGE td_ext_anp.liquidos_vendas_atual AS target
USING (
    SELECT
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        agente_regulado,
        codigo_produto,
        nome_produto,
        descricao_produto,
        regiao_origem,
        uf_origem,
        regiao_destinatario,
        uf_destino,
        mercado_destinatario,
        SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_vendas_atual
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_vendas_atual
    )
) AS source
ON  target.data                     = source.data
AND target.agente_regulado          = source.agente_regulado
AND target.codigo_produto           = source.codigo_produto
AND target.nome_produto             = source.nome_produto
AND target.descricao_produto        = source.descricao_produto
AND target.regiao_origem            = source.regiao_origem
AND target.uf_origem                = source.uf_origem
AND target.regiao_destinatario   = source.regiao_destinatario
AND target.uf_destino               = source.uf_destino
AND target.mercado_destinatario     = source.mercado_destinatario
WHEN MATCHED THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3
WHEN NOT MATCHED THEN
    INSERT (
        data,
        agente_regulado,
        codigo_produto,
        nome_produto,
        descricao_produto,
        regiao_origem,
        uf_origem,
        regiao_destinatario,
        uf_destino,
        mercado_destinatario,
        quantidade_produto_mil_m3,
        data_criacao
    )
    VALUES (
        source.data,
        source.agente_regulado,
        source.codigo_produto,
        source.nome_produto,
        source.descricao_produto,
        source.regiao_origem,
        source.uf_origem,
        source.regiao_destinatario,
        source.uf_destino,
        source.mercado_destinatario,
        source.quantidade_produto_mil_m3,
        source.data_criacao
    );
