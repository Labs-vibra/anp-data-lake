MERGE td_ext_anp.logistica_03 AS target
USING (
    SELECT
        PARSE_DATE('%Y/%m', periodo) AS periodo,
        produto,
        uf_origem,
        uf_destino,
        vendedor,
        comprador,
        SAFE_CAST(qtd_produto_liquido AS NUMERIC) AS qtd_produto_liquido,
        data_criacao
    FROM
        rw_ext_anp.logistica_03
    WHERE
        data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.logistica_03)
) AS source
ON source.periodo = target.periodo
AND source.produto = target.produto
AND source.uf_origem = target.uf_origem
AND source.uf_destino = target.uf_destino
AND source.vendedor = target.vendedor
AND source.comprador = target.comprador
AND source.data_criacao = target.data_criacao
WHEN MATCHED THEN
UPDATE SET
    qtd_produto_liquido = source.qtd_produto_liquido
WHEN NOT MATCHED THEN
INSERT (
    periodo,
    produto,
    uf_origem,
    uf_destino,
    vendedor,
    comprador,
    qtd_produto_liquido,
    data_criacao
)
VALUES (
    source.periodo,
    source.produto,
    source.uf_origem,
    source.uf_destino,
    source.vendedor,
    source.comprador,
    source.qtd_produto_liquido,
    source.data_criacao
);
