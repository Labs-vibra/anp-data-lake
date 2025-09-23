MERGE td_ext_anp.distribuidores_exercicio_atividade t
USING (
    SELECT
        FARM_FINGERPRINT(CONCAT(cnpj, '-', codigo_agente)) AS id,
        SAFE_CAST(codigo_agente AS NUMERIC) AS codigo_agente,
        SAFE_CAST(codigo_agente_i_simp AS NUMERIC) AS codigo_agente_i_simp,
        cnpj,
        nome_reduzido,
        razao_social,
        endereco_da_matriz,
        bairro,
        municipio,
        uf,
        cep,
        situacao,
        inicio_da_situacao,
        data_publicacao,
        tipo_de_ato,
        tipo_de_autorizacao,
        numero_do_ato,
        data_criacao
    FROM rw_ext_anp.distribuidores_exercicio_atividade
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