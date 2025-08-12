# CREATE TABLE IF NOT EXISTS rw_ext_anp_cbios_2020 (
    razao_social STRING,
    codigo_agente_regulado INT,
    cnpj STRING,
    meta_individual_2020 FLOAT64,
    meta_individual_2019_diaria FLOAT64,
    toal FLOAT64,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);