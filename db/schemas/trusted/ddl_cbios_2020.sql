CREATE TABLE IF NOT EXISTS td_ext_anp.cbios_2020 (
    razao_social STRING,
    codigo_agente_regulado STRING,
    cnpj STRING,
    somatorio_emissoes NUMERIC,
    participacao_mercado NUMERIC,
    meta_individual_2020 NUMERIC,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);