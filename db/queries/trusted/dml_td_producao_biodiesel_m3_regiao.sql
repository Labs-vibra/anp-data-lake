MERGE td_ext_anp.producao_biodiesel_m3_geral AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, mes, grande_regiao, unidade_federacao, produtor, produto)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', 
        CASE LOWER(TRIM(mes))
            WHEN 'jan' THEN '01'
            WHEN 'fev' THEN '02'
            WHEN 'mar' THEN '03'
            WHEN 'abr' THEN '04'
            WHEN 'mai' THEN '05'
            WHEN 'jun' THEN '06'
            WHEN 'jul' THEN '07'
            WHEN 'ago' THEN '08'
            WHEN 'set' THEN '09'
            WHEN 'out' THEN '10'
            WHEN 'nov' THEN '11'
            WHEN 'dez' THEN '12'
            ELSE mes
        END, '-01')) AS data,
        LOWER(REGEXP_REPLACE(NORMALIZE(grande_regiao, NFD), r'\pM', '')) AS grande_regiao,
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
WHEN MATCHED THEN
    UPDATE SET
        producao = source.producao,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        data,
        grande_regiao,
        producao,
        data_criacao
    )
    VALUES (
        source.id,
        source.data,
        source.grande_regiao,
        source.producao,
        source.data_criacao
    );