MERGE td_ext_anp.producao_biodiesel_m3_geral AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, mes, grande_regiao, unidade_federacao, produtor, produto)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
        LOWER(REGEXP_REPLACE(NORMALIZE(grande_regiao, NFD), r'\pM', '')) AS grande_regiao,
        LOWER(REGEXP_REPLACE(NORMALIZE(unidade_federacao, NFD), r'\pM', '')) AS unidade_federacao,
        LOWER(REGEXP_REPLACE(NORMALIZE(produtor, NFD), r'\pM', '')) AS produtor,
        LOWER(REGEXP_REPLACE(NORMALIZE(produto, NFD), r'\pM', '')) AS produto,
        IFNULL(SAFE_CAST(REPLACE(producao, ',', '.') AS NUMERIC), 0) AS producao,
        data_criacao
    FROM rw_ext_anp.producao_biodiesel_m3_geral
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.producao_biodiesel_m3_geral
    )
) AS source
ON  target.data                = source.data
AND target.grande_regiao       = source.grande_regiao
AND target.unidade_federacao   = source.unidade_federacao
AND target.produtor            = source.produtor
AND target.produto            = source.produto
WHEN MATCHED THEN
    UPDATE SET
        producao = source.producao,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        grande_regiao,
        unidade_federacao,
        produtor,
        produto,
        producao,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.grande_regiao,
        source.unidade_federacao,
        source.produtor,
        source.produto,
        source.producao,
        source.data_criacao
    );