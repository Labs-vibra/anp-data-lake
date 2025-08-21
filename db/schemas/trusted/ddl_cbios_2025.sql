CREATE TABLE IF NOT EXISTS td_ext_anp.cbios_2025 (
cnpj STRING,
codigo_agente_regulado STRING,
razao_social STRING,
meta_cnpe_2025_individualizada_cbio NUMERIC,
cbios_a_abater_2024 NUMERIC,
meta_individual_nao_cumprida_2024 NUMERIC,
meta_individual_2025_a_ser_cumprida_2025 NUMERIC,
data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);