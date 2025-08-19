MERGE td_ext_anp.logistica_01 AS target
USING (
    SELECT
        PARSE_DATE('%Y/%m', periodo) AS periodo,
        uf_origem,
        uf_destino,
        produto,
        classificacao,
        sub_classificacao,
        operacao,
        modal,
        SAFE_CAST(qtd_produto_liquido AS NUMERIC) AS qtd_produto_liquido,
        data_criacao
    FROM
        rw_ext_anp.logistica_01
    WHERE
        data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.logistica_01)
) AS source
    ON source.periodo = target.periodo
   AND source.uf_origem = target.uf_origem
   AND source.uf_destino = target.uf_destino
   AND source.produto = target.produto
   AND source.classificacao = target.classificacao
   AND source.sub_classificacao = target.sub_classificacao
   AND source.operacao = target.operacao
   AND source.modal = target.modal
   AND source.data_criacao = target.data_criacao
    WHEN MATCHED THEN
    UPDATE SET
        qtd_produto_liquido = source.qtd_produto_liquido
    WHEN NOT MATCHED THEN
    INSERT (
        periodo,
        uf_origem,
        uf_destino,
        produto,
        classificacao,
        sub_classificacao,
        operacao,
        modal,
        qtd_produto_liquido,
        data_criacao
    )
    VALUES (
        source.periodo,
        source.uf_origem,
        source.uf_destino,
        source.produto,
        source.classificacao,
        source.sub_classificacao,
        source.operacao,
        source.modal,
        source.qtd_produto_liquido,
        source.data_criacao
);
