# CREATE TABLE IF NOT EXISTS rw_ext_anp_cbios_2020 (
    razao_social STRING,
    codigo_agente_regulado STRING,
    cnpj STRING,
    meta_individual_2020 STRING,
    meta_individual_2019_diaria STRING,
    toal STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);