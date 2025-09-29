INSERT INTO rf_ext_anp.ft_anp_bio_producao_geral (
    anp_chv_ide,
    anp_dat_producao,
    anp_dsc_grande_regiao,
    anp_dsc_unidade_federacao,
    anp_dsc_produtor,
    anp_dsc_produto,
    anp_qtd_producao_m3,
    anp_qtd_producao_barris
)
SELECT
    id AS anp_chv_ide,
    data AS anp_dat_producao,
    grande_regiao AS anp_dsc_grande_regiao,
    unidade_federacao AS anp_dsc_unidade_federacao,
    produtor AS anp_dsc_produtor,
    produto AS anp_dsc_produto,
    producao AS anp_qtd_producao_m3,
    ROUND(producao * 6.28981, 2) AS anp_qtd_producao_barris
FROM td_ext_anp.producao_biodiesel_m3_geral;
