CREATE TABLE IF NOT EXISTS `td_ext_anp.distribuidores_exercicio_atividade`
(
    id INT64 OPTIONS(description="Identificador único gerado via FARM_FINGERPRINT"),
    codigo_agente NUMERIC OPTIONS(description="Código do agente"),
    codigo_agente_i_simp NUMERIC OPTIONS(description="Código do agente no sistema i-SIMP"),
    cnpj STRING OPTIONS(description="CNPJ da empresa distribuidora"),
    nome_reduzido STRING OPTIONS(description="Nome reduzido da empresa"),
    razao_social STRING OPTIONS(description="Razão social completa da empresa"),
    endereco_da_matriz STRING OPTIONS(description="Endereço da matriz da empresa"),
    bairro STRING OPTIONS(description="Bairro da empresa"),
    municipio STRING OPTIONS(description="Município da empresa"),
    uf STRING OPTIONS(description="Unidade federativa da empresa"),
    cep STRING OPTIONS(description="CEP da empresa"),
    situacao STRING OPTIONS(description="Situação da empresa"),
    inicio_da_situacao DATE OPTIONS(description="Início da situação da empresa"),
    data_publicacao DATE OPTIONS(description="Data de publicação da empresa"),
    tipo_de_ato STRING OPTIONS(description="Tipo de ato da empresa"),
    tipo_de_autorizacao STRING OPTIONS(description="Tipo de autorização da empresa"),
    numero_do_ato NUMERIC OPTIONS(description="Número do ato da empresa"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão do registro na camada trusted")
)
PARTITION BY DATE(data_ingestao_td)
OPTIONS(description="Tabela de Distribuidores por Exercício e Atividade")
