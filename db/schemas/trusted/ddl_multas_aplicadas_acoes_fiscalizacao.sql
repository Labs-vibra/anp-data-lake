CREATE TABLE IF NOT EXISTS td_ext_anp.multas_aplicadas_acoes_fiscalizacao (
    id INT64 OPTIONS(description="Identificador único gerado via FARM_FINGERPRINT"),
    status_processo STRING OPTIONS(description="Status do processo de fiscalização"),
    superintendencia STRING OPTIONS(description="Superintendência responsável"),
    numero_processo STRING OPTIONS(description="Número do processo de fiscalização"),
    auto_infracao STRING OPTIONS(description="Número do auto de infração"),
    numero_duf STRING OPTIONS(description="Número do DUF (Documento Único de Fiscalização)"),
    cnpj_cpf STRING OPTIONS(description="CNPJ ou CPF do autuado"),
    razao_social STRING OPTIONS(description="Razão social ou nome do autuado"),
    data_transito_julgado DATE OPTIONS(description="Data do trânsito em julgado"),
    vencimento DATE OPTIONS(description="Data de vencimento da multa"),
    valor_multa_aplicada NUMERIC OPTIONS(description="Valor da multa aplicada em reais"),
    valor_total_pago NUMERIC OPTIONS(description="Valor total pago da multa em reais"),
    ano_referencia STRING OPTIONS(description="Ano ou período de referência dos dados"),
    arquivo_origem STRING OPTIONS(description="Nome do arquivo de origem dos dados"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingestao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de ingestão do registro na camada trusted")
) PARTITION BY DATE(data_ingestao_td)
OPTIONS(description="Tabela trusted de multas aplicadas e ações de fiscalização da ANP");
