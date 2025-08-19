MERGE td_ext_anp.logistica_02 AS target
USING (
    SELECT
        PARSE_DATE('%Y/%m', periodo) AS periodo,
        uf_destino,
        produto,
        vendedor,
        SAFE_CAST(quantidade_produto_liquido AS NUMERIC) AS qtd_produto_liquido,
        data_criacao
    FROM
        rw_ext_anp.logistica_02
    WHERE
        data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.logistica_02)
) AS source
    ON source.periodo = target.periodo
   AND source.uf_destino = target.uf_destino
   AND source.produto = target.produto
   AND source.vendedor = target.vendedor
   AND source.data_criacao = target.data_criacao
    WHEN MATCHED THEN
    UPDATE SET
        qtd_produto_liquido = source.qtd_produto_liquido
    WHEN NOT MATCHED THEN
    INSERT (
        periodo,
        uf_destino,
        produto,
        vendedor,
        qtd_produto_liquido,
        data_criacao
    )
    VALUES (
        source.periodo,
        source.uf_destino,
        source.produto,
        source.vendedor,
        source.qtd_produto_liquido,
        source.data_criacao
);
