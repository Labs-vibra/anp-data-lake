CREATE TABLE IF NOT EXISTS `rw_ext_anp.codigos_instalacao`
(
  cod_instalacao STRING OPTIONS(description="Código da instalação"),
  num_cnpj STRING OPTIONS(description="CNPJ da instalação"),
  nom_razao_social STRING OPTIONS(description="Razão social da instalação"),
  num_cep STRING OPTIONS(description="CEP"),
  nom_titulo STRING OPTIONS(description="Título da instalação"),
  txt_endereco STRING OPTIONS(description="Endereço completo"),
  num_numero STRING OPTIONS(description="Número"),
  txt_complemento STRING OPTIONS(description="Complemento do endereço"),
  nom_bairro STRING OPTIONS(description="Bairro"),
  nom_municipio STRING OPTIONS(description="Município"),
  nom_estado STRING OPTIONS(description="Estado"),
  num_autorizacao STRING OPTIONS(description="Número da autorização"),
  dat_publicacao STRING OPTIONS(description="Data da publicação no formato original"),
  txt_status STRING OPTIONS(description="Status da instalação"),
  txt_tipo_instalacao STRING OPTIONS(description="Tipo da instalação"),
  txt_identificacao_instalacao STRING OPTIONS(description="Identificação da instalação"),
  nom_reduzido STRING OPTIONS(description="Nome reduzido"),
  num_cnpj_administrador_base STRING OPTIONS(description="CNPJ do administrador da base"),
  dat_mesano_vigencia_inicial STRING OPTIONS(description="Data de vigência inicial (formato original)"),
  dat_mesano_vigencia_final STRING OPTIONS(description="Data de vigência final (formato original)"),
  data_versao STRING OPTIONS(description="Versão dos dados"),
  data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data e hora de ingestão dos dados")
)
PARTITION BY DATE(data_criacao)
OPTIONS(description="Tabela Raw contendo os códigos de instalação da ANP, conforme fonte original, sem transformações.");