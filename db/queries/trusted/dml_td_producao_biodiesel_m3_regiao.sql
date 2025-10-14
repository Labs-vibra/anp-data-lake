MERGE td_ext_anp.producao_biodiesel_m3_regiao AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, mes, grande_regiao)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', 
            CASE LOWER(TRIM(mes))
                WHEN 'janeiro' THEN '01'
                WHEN 'fevereiro' THEN '02'
                WHEN 'março' THEN '03'
                WHEN 'abril' THEN '04'
                WHEN 'maio' THEN '05'
                WHEN 'junho' THEN '06'
                WHEN 'julho' THEN '07'
                WHEN 'agosto' THEN '08'
                WHEN 'setembro' THEN '09'
                WHEN 'outubro' THEN '10'
                WHEN 'novembro' THEN '11'
                WHEN 'dezembro' THEN '12'
                ELSE mes
            END, '-01')) AS data,
        LOWER(REGEXP_REPLACE(NORMALIZE(grande_regiao, NFD), r'\pM', '')) AS grande_regiao,
        IFNULL(SAFE_CAST(REPLACE(producao, ',', '.') AS NUMERIC), 0) AS producao,
        data_criacao
    FROM rw_ext_anp.producao_biodiesel_m3_regiao
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.producao_biodiesel_m3_regiao
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