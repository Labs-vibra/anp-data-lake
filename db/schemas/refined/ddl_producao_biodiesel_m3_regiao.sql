CREATE TABLE IF NOT EXISTS rf_ext_anp.ft_anp_bio_producao_regiao (
    anp_chv_ide INT64 OPTIONS(description="Identificador único do registro"),
    anp_dat_producao DATE OPTIONS(description="Data da produção"),
    anp_dsc_grande_regiao STRING OPTIONS(description="Grande região do Brasil"),
    anp_qtd_producao_m3 NUMERIC OPTIONS(description="Quantidade produzida em metros cúbicos (m³)"),
    anp_qtd_producao_barris NUMERIC OPTIONS(description="Quantidade produzida convertida para barris"),
    data_ingestao_td TIMESTAMP (description="Data da inserção dos dados na camada trusted"),
    data_ingestao_rf TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de inserção dos dados na camada refined")
)
PARTITION BY DATE(data_ingestao_rf)
OPTIONS(description="Tabela de fatos contendo a produção de biodiesel em m³ e barris, por região, convertida e modelada para consumo analítico.");
