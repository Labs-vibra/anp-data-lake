WITH regex_acentos AS (
  SELECT r'[\u0300-\u036f]' AS rx -- regex para remover diacríticos após normalização NFD
)
MERGE td_ext_anp.distribuidores_exercicio_atividade t
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(
            LOWER(REGEXP_REPLACE(NORMALIZE(cnpj, NFD), rx, '')),
            '-', SAFE_CAST(codigo_agente AS STRING)
        )) AS id,
        SAFE_CAST(codigo_agente AS NUMERIC) AS codigo_agente,
        SAFE_CAST(codigo_agente_i_simp AS NUMERIC) AS codigo_agente_i_simp,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(cnpj, NFD), rx, ''))) AS cnpj,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(nome_reduzido, NFD), rx, ''))) AS nome_reduzido,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(razao_social, NFD), rx, ''))) AS razao_social,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(endereco_da_matriz, NFD), rx, ''))) AS endereco_da_matriz,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(bairro, NFD), rx, ''))) AS bairro,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(municipio, NFD), rx, ''))) AS municipio,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(uf, NFD), rx, ''))) AS uf,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(cep, NFD), rx, ''))) AS cep,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(situacao, NFD), rx, ''))) AS situacao,
        SAFE_CAST(inicio_da_situacao AS DATE) AS inicio_da_situacao,
        SAFE_CAST(data_publicacao AS DATE) AS data_publicacao,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(tipo_de_ato, NFD), rx, ''))) AS tipo_de_ato,
        TRIM(LOWER(REGEXP_REPLACE(NORMALIZE(tipo_de_autorizacao, NFD), rx, ''))) AS tipo_de_autorizacao,
        SAFE_CAST(numero_do_ato AS NUMERIC) AS numero_do_ato,
        data_criacao
    FROM rw_ext_anp.distribuidores_exercicio_atividade, regex_acentos
    WHERE data_criacao = (
        SELECT MAX(data_criacao)
        FROM rw_ext_anp.aposentadoria_cbios
    )
) r
ON t.id = r.id
WHEN MATCHED THEN
  UPDATE SET
    codigo_agente = r.codigo_agente,
    codigo_agente_i_simp = r.codigo_agente_i_simp,
    cnpj = r.cnpj,
    nome_reduzido = r.nome_reduzido,
    razao_social = r.razao_social,
    endereco_da_matriz = r.endereco_da_matriz,
    bairro = r.bairro,
    municipio = r.municipio,
    uf = r.uf,
    cep = r.cep,
    situacao = r.situacao,
    inicio_da_situacao = r.inicio_da_situacao,
    data_publicacao = r.data_publicacao,
    tipo_de_ato = r.tipo_de_ato,
    tipo_de_autorizacao = r.tipo_de_autorizacao,
    numero_do_ato = r.numero_do_ato,
    data_criacao = r.data_criacao
WHEN NOT MATCHED THEN
  INSERT (
    id, codigo_agente, codigo_agente_i_simp, cnpj, nome_reduzido, razao_social,
    endereco_da_matriz, bairro, municipio, uf, cep, situacao, inicio_da_situacao,
    data_publicacao, tipo_de_ato, tipo_de_autorizacao, numero_do_ato, data_criacao
  )
  VALUES (
    r.id, r.codigo_agente, r.codigo_agente_i_simp, r.cnpj, r.nome_reduzido, r.razao_social,
    r.endereco_da_matriz, r.bairro, r.municipio, r.uf, r.cep, r.situacao, r.inicio_da_situacao,
    r.data_publicacao, r.tipo_de_ato, r.tipo_de_autorizacao, r.numero_do_ato, r.data_criacao
  );