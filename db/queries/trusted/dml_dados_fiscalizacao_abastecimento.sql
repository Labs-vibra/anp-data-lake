MERGE td_ext_anp.dados_fiscalizacao_do_abastecimento AS target
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(agente_economico, '-', cnpj_ou_cpf, '-', segmento_fiscalizado, '-', numero_do_documento, '-', procedimento_de_fiscalizacao)) AS id,
        LOWER(TRIM(uf)) AS uf,
        LOWER(TRIM(municipio)) AS municipio,
        LOWER(TRIM(bairro)) AS bairro,
        LOWER(TRIM(endereco)) AS endereco,
        REGEXP_REPLACE(cnpj_ou_cpf, r'[^0-9]', '') AS cnpj_ou_cpf,
        REGEXP_REPLACE(LOWER(TRIM(NORMALIZE(agente_economico, NFD))), r'[^a-zA-Z0-9_\s\-\.]', '') AS agente_economico,
        LOWER(TRIM(segmento_fiscalizado)) AS segmento_fiscalizado,
        PARSE_DATE('%Y-%m-%d', data_do_df) AS data_do_df,
        IFNULL(SAFE_CAST(NULLIF(numero_do_documento, '')) AS NUMERIC, 0) AS numero_do_documento,
        LOWER(TRIM(procedimento_de_fiscalizacao)) AS procedimento_de_fiscalizacao,
        LOWER(TRIM(resultado)) AS resultado,
        data_criacao
    FROM rw_ext_anp.dados_fiscalizacao_do_abastecimento
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.dados_fiscalizacao_do_abastecimento
    )
) AS source
ON  target.id                     = source.id
AND target.uf                     = source.uf
AND target.municipio              = source.municipio
AND target.bairro                 = source.bairro
AND target.endereco               = source.endereco
AND target.cnpj_ou_cpf            = source.cnpj_ou_cpf
AND target.agente_economico       = source.agente_economico
AND target.segmento_fiscalizado   = source.segmento_fiscalizado
AND target.data_do_df             = source.data_do_df
AND target.numero_do_documento    = source.numero_do_documento
WHEN MATCHED AND (
    target.resultado IS DISTINCT FROM source.resultado
    OR target.procedimento_de_fiscalizacao IS DISTINCT FROM source.procedimento_de_fiscalizacao
) THEN
    UPDATE SET
        resultado = source.resultado,
        procedimento_de_fiscalizacao = source.procedimento_de_fiscalizacao,
        data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
    INSERT (
        id,
        uf,
        municipio,
        bairro,
        endereco,
        cnpj_ou_cpf,
        agente_economico,
        segmento_fiscalizado,
        data_do_df,
        numero_do_documento,
        procedimento_de_fiscalizacao,
        resultado,
        data_criacao
    )
    VALUES (
        source.id,
        source.uf,
        source.municipio,
        source.bairro,
        source.endereco,
        source.cnpj_ou_cpf,
        source.agente_economico,
        source.segmento_fiscalizado,
        source.data_do_df,
        source.numero_do_documento,
        source.procedimento_de_fiscalizacao,
        source.resultado,
        source.data_criacao
    );

