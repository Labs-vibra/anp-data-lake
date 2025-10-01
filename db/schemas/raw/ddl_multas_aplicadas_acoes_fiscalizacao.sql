CREATE TABLE IF NOT EXISTS rw_ext_anp.multas_aplicadas_acoes_fiscalizacao (
    status_processo STRING OPTIONS(description="Status do processo de fiscalização"),
    superintendencia STRING OPTIONS(description="Superintendência responsável"),
    numero_processo STRING OPTIONS(description="Número do processo de fiscalização"),
    auto_infracao STRING OPTIONS(description="Número do auto de infração"),
    numero_duf STRING OPTIONS(description="Número do DUF (Documento Único de Fiscalização)"),
    cnpj_cpf STRING OPTIONS(description="CNPJ ou CPF do autuado"),
    razao_social STRING OPTIONS(description="Razão social ou nome do autuado"),
    data_transito_julgado STRING OPTIONS(description="Data do trânsito em julgado"),
    vencimento STRING OPTIONS(description="Data de vencimento da multa"),
    valor_multa_aplicada STRING OPTIONS(description="Valor da multa aplicada em reais"),
    valor_total_pago STRING OPTIONS(description="Valor total pago da multa em reais"),
    ano_referencia STRING OPTIONS(description="Ano ou período de referência dos dados"),
    arquivo_origem STRING OPTIONS(description="Nome do arquivo de origem dos dados"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na camada raw")
) PARTITION BY DATE(data_criacao)
OPTIONS(description="Tabela raw de multas aplicadas e ações de fiscalização da ANP");
