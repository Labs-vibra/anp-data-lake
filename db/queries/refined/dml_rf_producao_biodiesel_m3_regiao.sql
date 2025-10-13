MERGE rf_ext_anp.ft_anp_bio_producao_regiao AS target
USING (
    SELECT
        id AS anp_chv_id,
        data AS anp_dat_producao,
        grande_regiao AS anp_dsc_grande_regiao,
        producao AS anp_qtd_producao_m3,
        producao * 6.28981 AS anp_qtd_producao_barris,
        data_ingestao_td
    FROM td_ext_anp.producao_biodiesel_m3_regiao
    WHERE data_ingestao_td = (
        SELECT MAX(data_ingestao_td)
        FROM td_ext_anp.producao_biodiesel_m3_regiao
    )
) AS source
ON  target.anp_dat_producao         = source.anp_dat_producao
AND target.anp_dsc_grande_regiao    = source.anp_dsc_grande_regiao
WHEN MATCHED THEN
    UPDATE SET
        anp_qtd_producao_m3     = source.anp_qtd_producao_m3,
        anp_qtd_producao_barris = source.anp_qtd_producao_barris,
        data_ingestao_td        = source.data_ingestao_td
WHEN NOT MATCHED THEN
    INSERT (
        anp_chv_id,
        anp_dat_producao,
        anp_dsc_grande_regiao,
        anp_qtd_producao_m3,
        anp_qtd_producao_barris,
        data_ingestao_td
    )
    VALUES (
        source.anp_chv_id,
        source.anp_dat_producao,
        source.anp_dsc_grande_regiao,
        source.anp_qtd_producao_m3,
        source.anp_qtd_producao_barris,
        source.data_ingestao_td
    );