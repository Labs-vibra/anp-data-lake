MERGE td_ext_anp.liquidos_entregas_distribuidor_atual AS target
USING (
    SELECT
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', LPAD(mes, 2, '0'), '-01')) AS data,
        distribuidor,
        codigo_produto,
        nome_produto,
        regiao,
        SAFE_CAST(REPLACE(quantidade_produto_mil_m3, ',', '.') AS NUMERIC) AS quantidade_produto_mil_m3,
        data_criacao
    FROM rw_ext_anp.liquidos_entregas_distribuidor_atual
) AS source
ON target.data = source.data
   AND target.distribuidor = source.distribuidor
   AND target.codigo_produto = source.codigo_produto
   AND target.regiao = source.regiao
WHEN MATCHED THEN
    UPDATE SET
        target.nome_produto = source.nome_produto,
        target.quantidade_produto_mil_m3 = source.quantidade_produto_mil_m3,
        target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        `data`, distribuidor, codigo_produto, nome_produto, regiao, quantidade_produto_mil_m3, data_criacao
    )
    VALUES (
        source.data, source.distribuidor, source.codigo_produto, source.nome_produto, source.regiao, source.quantidade_produto_mil_m3, source.data_criacao
    );