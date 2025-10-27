MERGE td_ext_anp.producao_biodiesel_m3_regiao AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(ano, mes, grande_regiao)) AS id,
        PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-',
            CASE UPPER(TRIM(mes))
                WHEN 'JAN' THEN '01'
                WHEN 'FEV' THEN '02'
                WHEN 'MAR' THEN '03'
                WHEN 'ABR' THEN '04'
                WHEN 'MAI' THEN '05'
                WHEN 'JUN' THEN '06'
                WHEN 'JUL' THEN '07'
                WHEN 'AGO' THEN '08'
                WHEN 'SET' THEN '09'
                WHEN 'OUT' THEN '10'
                WHEN 'NOV' THEN '11'
                WHEN 'DEZ' THEN '12'
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