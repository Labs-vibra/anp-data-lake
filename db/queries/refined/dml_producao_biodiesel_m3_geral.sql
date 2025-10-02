MERGE rf_ext_anp.ft_anp_bio_producao_geral AS target
USING (
    SELECT
        id AS anp_chv_id,
        data AS anp_dat_producao,
        grande_regiao AS anp_dsc_grande_regiao,
        unidade_federacao AS anp_dsc_unidade_federacao,
        produtor AS anp_dsc_produtor,
        produto AS anp_dsc_produto,
        producao AS anp_qtd_producao_m3,
        producao * 6.28981 AS anp_qtd_producao_barris,
        data_ingestao_td
    FROM td_ext_anp.producao_biodiesel_m3_geral
    WHERE data_criacao = (
        SELECT MAX(data_ingestao_td)
        FROM td_ext_anp.producao_biodiesel_m3_geral
    )
) AS source
ON  target.anp_dat_producao         = source.anp_dat_producao
AND target.anp_dsc_grande_regiao    = source.anp_dsc_grande_regiao
AND target.anp_dsc_unidade_federacao= source.anp_dsc_unidade_federacao
AND target.anp_dsc_produtor         = source.anp_dsc_produtor
AND target.anp_dsc_produto          = source.anp_dsc_produto
WHEN MATCHED THEN
    UPDATE SET
        anp_qtd_producao_m3     = source.anp_qtd_producao_m3,
        anp_qtd_producao_barris = source.anp_qtd_producao_barris,
        data_ingestao_td        = source.data_ingestao_td
WHEN NOT MATCHED THEN
    INSERT (
        anp_chv_ide,
        anp_dat_producao,
        anp_dsc_grande_regiao,
        anp_dsc_unidade_federacao,
        anp_dsc_produtor,
        anp_dsc_produto,
        anp_qtd_producao_m3,
        anp_qtd_producao_barris,
        data_ingestao_td
    )
    VALUES (
        source.anp_chv_ide,
        source.anp_dat_producao,
        source.anp_dsc_grande_regiao,
        source.anp_dsc_unidade_federacao,
        source.anp_dsc_produtor,
        source.anp_dsc_produto,
        source.anp_qtd_producao_m3,
        source.anp_qtd_producao_barris,
        source.data_ingestao_td
    );
