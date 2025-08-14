CREATE TABLE IF NOT EXISTS td_ext_anp.logistics_01 (
    veco_dat_periodo DATE,
    veco_txt_origem STRING,
    veco_txt_destino STRING,
    veco_nom_produto STRING,
    veco_classificacao STRING,
    veco_sub_classificacao STRING,
    veco_operacao STRING,
    veco_modal STRING,
    veco_qtd_produto FLOAT64,
    veco_dat_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(veco_dat_criacao);
