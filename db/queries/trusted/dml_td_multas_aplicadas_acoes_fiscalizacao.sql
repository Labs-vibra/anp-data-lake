MERGE td_ext_anp.multas_aplicadas_acoes_fiscalizacao AS t
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(
            COALESCE(TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(numero_processo, NFD), r'[\u0300-\u036f]', ''))), ''),
            '-', COALESCE(TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(auto_infracao, NFD), r'[\u0300-\u036f]', ''))), ''),
            '-', COALESCE(TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(numero_duf, NFD), r'[\u0300-\u036f]', ''))), ''),
            '-', COALESCE(TRIM(REGEXP_REPLACE(NORMALIZE(cnpj_cpf, NFD), r'[\u0300-\u036f]', '')), ''),
            '-', COALESCE(TRIM(ano_referencia), '')
        )) AS id,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(status_processo, NFD), r'[\u0300-\u036f]', ''))) AS status_processo,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(superintendencia, NFD), r'[\u0300-\u036f]', ''))) AS superintendencia,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(numero_processo, NFD), r'[\u0300-\u036f]', ''))) AS numero_processo,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(auto_infracao, NFD), r'[\u0300-\u036f]', ''))) AS auto_infracao,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(numero_duf, NFD), r'[\u0300-\u036f]', ''))) AS numero_duf,
        TRIM(REGEXP_REPLACE(NORMALIZE(cnpj_cpf, NFD), r'[\u0300-\u036f]', '')) AS cnpj_cpf,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(razao_social, NFD), r'[\u0300-\u036f]', ''))) AS razao_social,
        SAFE.PARSE_DATE('%d/%m/%Y', data_transito_julgado) AS data_transito_julgado,
        SAFE.PARSE_DATE('%d/%m/%Y', vencimento) AS vencimento,
        SAFE_CAST(
            TRIM(REPLACE(REPLACE(REPLACE(REPLACE(valor_multa_aplicada, 'R$', ''), '.', ''), ',', '.'), ' ', ''))
        AS NUMERIC) AS valor_multa_aplicada,
        SAFE_CAST(
            TRIM(REPLACE(REPLACE(REPLACE(REPLACE(valor_total_pago, 'R$', ''), '.', ''), ',', '.'), ' ', ''))
        AS NUMERIC) AS valor_total_pago,
        TRIM(ano_referencia) AS ano_referencia,
        TRIM(arquivo_origem) AS arquivo_origem,
        data_criacao
    FROM rw_ext_anp.multas_aplicadas_acoes_fiscalizacao
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.multas_aplicadas_acoes_fiscalizacao
    )
) r
ON t.id = r.id
WHEN MATCHED THEN
  UPDATE SET
    status_processo = r.status_processo,
    superintendencia = r.superintendencia,
    numero_processo = r.numero_processo,
    auto_infracao = r.auto_infracao,
    numero_duf = r.numero_duf,
    cnpj_cpf = r.cnpj_cpf,
    razao_social = r.razao_social,
    data_transito_julgado = r.data_transito_julgado,
    vencimento = r.vencimento,
    valor_multa_aplicada = r.valor_multa_aplicada,
    valor_total_pago = r.valor_total_pago,
    ano_referencia = r.ano_referencia,
    arquivo_origem = r.arquivo_origem,
    data_criacao = r.data_criacao
WHEN NOT MATCHED THEN
  INSERT (
    id, status_processo, superintendencia, numero_processo, auto_infracao,
    numero_duf, cnpj_cpf, razao_social, data_transito_julgado, vencimento,
    valor_multa_aplicada, valor_total_pago, ano_referencia, arquivo_origem, data_criacao
  )
  VALUES (
    r.id, r.status_processo, r.superintendencia, r.numero_processo, r.auto_infracao,
    r.numero_duf, r.cnpj_cpf, r.razao_social, r.data_transito_julgado, r.vencimento,
    r.valor_multa_aplicada, r.valor_total_pago, r.ano_referencia, r.arquivo_origem, r.data_criacao
  );