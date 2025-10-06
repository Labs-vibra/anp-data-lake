CREATE TABLE IF NOT EXISTS rw_ext_anp.dados_fiscalizacao_do_abastecimento (
    uf STRING OPTIONS(description="Sigla da Unidade da Federação onde está estabelecido o agente fiscalizado."),
    municipio STRING OPTIONS(description="Nome do município onde está estabelecido o agente fiscalizado."),
    bairro STRING OPTIONS(description="Nome do bairro onde está estabelecido o agente fiscalizado"),
    endereco STRING OPTIONS(description="Logradouro onde o agente fiscalizado está estabelecido, bem como demais complementos necessários à correta identificação do local."),
    cnpj_ou_cpf STRING OPTIONS(description="Nº do Cadastro Nacional da Pessoa Jurídica (CNPJ) do agente fiscalizado. Caso o estabelecimento não tenha CNPJ, o campo apresentará o nº do Cadastro de Pessoa Física (CPF) do responsável pelo estabelecimento."),
    agente_economico STRING OPTIONS(description="Nome devidamente registrado sob o qual a pessoa jurídica fiscalizada se individualiza e exerce suas atividades."),
    segmento_fiscalizado STRING OPTIONS(description="Atividade econômica integrante da indústria do petróleo, do gás natural e dos biocombustíveis exercida pelo agente fiscalizado."),
    data_do_df STRING OPTIONS(description="Data do Documento de Fiscalização (DF)."),
    numero_do_documento STRING OPTIONS(description="Nº do Documento de Fiscalização utilizado pela autoridade competente da ANP ou do órgão público conveniado designado para as atividades de fiscalização."),
    procedimento_de_fiscalizacao STRING OPTIONS(description="Descrição dos procedimentos adotados durante a fiscalização, podendo ser: Boletim de Fiscalização; Auto de Infração; Auto de Interdição; Auto de Apreensão; Notificação; Termo de Coleta de Amostra; Termo Fiel Depositário; Certidão; Medida Reparadora de Conduta; Termo Final de Medida Cautelar; Ato de Início de Suspensão; Ato de Término de Suspensão."),
    resultado STRING OPTIONS(description="Descrição dos fatos verificados durante a fiscalização, conforme o procedimento de fiscalização adotado"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de inserção do registro na camada raw.")
) PARTITION BY DATE(data_criacao) OPTIONS (
  description = "O conjunto de dados contém informações sobre as ações de fiscalização da ANP nos diversos segmentos que compõe o abastecimento nacional de combustíveis. Dentre as informações apresentadas estão: números dos documentos de fiscalização; identificação dos agentes fiscalizados; locais onde as ações foram realizadas; resultados das fiscalizações etc."
);

