CREATE TABLE IF NOT EXISTS `td_ext_anp.codigos_instalacao`
(
  id INT64 OPTIONS(description="Identificador único gerado via FARM_FINGERPRINT"),
  cod_instalacao STRING OPTIONS(description="Código da instalação"),
  num_cnpj STRING OPTIONS(description="CNPJ da instalação"),
  nom_razao_social STRING OPTIONS(description="Razão social da instalação, normalizada"),
  num_cep STRING OPTIONS(description="CEP"),
  txt_endereco STRING OPTIONS(description="Endereço normalizado"),
  num_numero NUMERIC OPTIONS(description="Número"),
  txt_complemento STRING OPTIONS(description="Complemento do endereço, normalizado"),
  nom_bairro STRING OPTIONS(description="Bairro, normalizado"),
  nom_municipio STRING OPTIONS(description="Município, normalizado"),
  nom_estado STRING OPTIONS(description="Estado, normalizado"),
  num_autorizacao STRING OPTIONS(description="Número da autorização"),
  dat_publicacao DATE OPTIONS(description="Data da publicação"),
  txt_status STRING OPTIONS(description="Status da instalação, normalizado"),
  txt_tipo_instalacao STRING OPTIONS(description="Tipo da instalação, normalizado"),
  nom_reduzido STRING OPTIONS(description="Nome reduzido, normalizado"),
  num_cnpj_administrador_base STRING OPTIONS(description="CNPJ do administrador da base"),
  dat_mesano_vigencia_inicial DATE OPTIONS(description="Data de vigência inicial"),
  dat_mesano_vigencia_final DATE OPTIONS(description="Data de vigência final"),
  data_versao TIMESTAMP OPTIONS(description="Versão dos dados"),
  data_criacao TIMESTAMP DEFAULT OPTIONS(description="Data de criação do registro na camada raw"),
  data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão na camada trusted")
)
PARTITION BY DATE(data_ingestao_td)
OPTIONS(description="Tabela Trusted contendo os códigos de instalação da ANP, com dados normalizados, tipados e deduplicados.");
