MERGE td_ext_anp.liquidos_entrega_historico AS target
USING (
    SELECT
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        fornecedor_destino,
        distribuidor_origem,
        codigo_produto,
        nome_produto,
        regiao_origem,
        uf_origem,
        localidade_destino,
        regiao_destinatario,
        uf_destino,
        SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_entrega_historico
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.liquidos_entrega_historico
    )
) AS source
ON source.data = target.data
   AND source.fornecedor_destino = target.fornecedor_destino
   AND source.distribuidor_origem = target.distribuidor_origem
   AND source.codigo_produto = target.codigo_produto
   AND source.nome_produto = target.nome_produto
   AND source.regiao_origem = target.regiao_origem
   AND source.uf_origem = target.uf_origem
   AND source.localidade_destino = target.localidade_destino
   AND source.regiao_destinatario = target.regiao_destinatario
   AND source.uf_destino = target.uf_destino
WHEN MATCHED THEN
    UPDATE SET
        quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3
WHEN NOT MATCHED THEN
    INSERT (
        data,
        fornecedor_destino,
        distribuidor_origem,
        codigo_produto,
        nome_produto,
        regiao_origem,
        uf_origem,
        localidade_destino,
        regiao_destinatario,
        uf_destino,
        quantidade_produto_mil_m3,
        data_criacao
    )
    VALUES (
        source.data,
        source.fornecedor_destino,
        source.distribuidor_origem,
        source.codigo_produto,
        source.nome_produto,
        source.regiao_origem,
        source.uf_origem,
        source.localidade_destino,
        source.regiao_destinatario,
        source.uf_destino,
        source.quantidade_produto_mil_m3,
        source.data_criacao
    );
