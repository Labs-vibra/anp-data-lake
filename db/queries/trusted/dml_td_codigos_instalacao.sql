MERGE td_ext_anp.codigos_instalacao AS target
USING (
SELECT
    FARM_FINGERPRINT(CONCAT(cod_instalacao, num_cnpj)) AS id,
    cod_instalacao,
    num_cnpj,
    LOWER(REGEXP_REPLACE(NORMALIZE(nom_razao_social, NFD), r'\pM', '')) AS nom_razao_social,
    num_cep,
    LOWER(REGEXP_REPLACE(NORMALIZE(txt_endereco, NFD), r'\pM', '')) AS txt_endereco,
    SAFE_CAST(num_numero AS NUMERIC) AS num_numero,
    LOWER(REGEXP_REPLACE(NORMALIZE(txt_complemento, NFD), r'\pM', '')) AS txt_complemento,
    LOWER(REGEXP_REPLACE(NORMALIZE(nom_bairro, NFD), r'\pM', '')) AS nom_bairro,
    LOWER(REGEXP_REPLACE(NORMALIZE(nom_municipio, NFD), r'\pM', '')) AS nom_municipio,
    LOWER(REGEXP_REPLACE(NORMALIZE(nom_estado, NFD), r'\pM', '')) AS nom_estado,
    num_autorizacao,
    SAFE.PARSE_DATE('%d/%m/%Y', SPLIT(dat_publicacao, ',')[OFFSET(0)]) AS dat_publicacao,
    LOWER(REGEXP_REPLACE(NORMALIZE(txt_status, NFD), r'\pM', '')) AS txt_status,
    LOWER(REGEXP_REPLACE(NORMALIZE(txt_tipo_instalacao, NFD), r'\pM', '')) AS txt_tipo_instalacao,
    LOWER(REGEXP_REPLACE(NORMALIZE(nom_reduzido, NFD), r'\pM', '')) AS nom_reduzido,
    num_cnpj_administrador_base,
    SAFE.PARSE_DATE('%Y%m', dat_mesano_vigencia_inicial || '01') AS dat_mesano_vigencia_inicial,
    SAFE.PARSE_DATE('%Y%m', dat_mesano_vigencia_final || '01') AS dat_mesano_vigencia_final,
    SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M', data_versao) AS data_versao,
    data_criacao
FROM
    rw_ext_anp.codigos_instalacao
WHERE
    data_criacao = (SELECT MAX(data_criacao) FROM rw_ext_anp.codigos_instalacao)
) AS source
ON source.cod_instalacao = target.cod_instalacao
AND source.num_cnpj = target.num_cnpj
AND source.nom_razao_social = target.nom_razao_social
WHEN MATCHED THEN
UPDATE SET
    target.num_cep = source.num_cep,
    target.txt_endereco = source.txt_endereco,
    target.num_numero = source.num_numero,
    target.txt_complemento = source.txt_complemento,
    target.nom_bairro = source.nom_bairro,
    target.nom_municipio = source.nom_municipio,
    target.nom_estado = source.nom_estado,
    target.num_autorizacao = source.num_autorizacao,
    target.dat_publicacao = source.dat_publicacao,
    target.txt_status = source.txt_status,
    target.txt_tipo_instalacao = source.txt_tipo_instalacao,
    target.nom_reduzido = source.nom_reduzido,
    target.num_cnpj_administrador_base = source.num_cnpj_administrador_base,
    target.dat_mesano_vigencia_inicial = source.dat_mesano_vigencia_inicial,
    target.dat_mesano_vigencia_final = source.dat_mesano_vigencia_final,
    target.data_versao = source.data_versao,
    target.data_criacao = source.data_criacao
WHEN NOT MATCHED THEN
INSERT (
    id,
    cod_instalacao,
    num_cnpj,
    nom_razao_social,
    num_cep,
    txt_endereco,
    num_numero,
    txt_complemento,
    nom_bairro,
    nom_municipio,
    nom_estado,
    num_autorizacao,
    dat_publicacao,
    txt_status,
    txt_tipo_instalacao,
    nom_reduzido,
    num_cnpj_administrador_base,
    dat_mesano_vigencia_inicial,
    dat_mesano_vigencia_final,
    data_versao,
    data_criacao
) VALUES (
    source.id,
    source.cod_instalacao,
    source.num_cnpj,
    source.nom_razao_social,
    source.num_cep,
    source.txt_endereco,
    source.num_numero,
    source.txt_complemento,
    source.nom_bairro,
    source.nom_municipio,
    source.nom_estado,
    source.num_autorizacao,
    source.dat_publicacao,
    source.txt_status,
    source.txt_tipo_instalacao,
    source.nom_reduzido,
    source.num_cnpj_administrador_base,
    source.dat_mesano_vigencia_inicial,
    source.dat_mesano_vigencia_final,
    source.data_versao,
    source.data_criacao
);