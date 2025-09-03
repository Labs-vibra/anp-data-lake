CREATE TABLE IF NOT EXISTS rw_ext_anp.cbios_2020 (
    razao_social STRING,
    codigo_agente_regulado STRING,
    cnpj STRING,
    somatorio_emissoes STRING,
    participacao_mercado STRING,
    meta_individual_2020 STRING,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(data_criacao);