import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
FILES_BUCKET_DESTINATION = "anp/multas_aplicadas_acoes_fiscalizacao"

BQ_DATASET = "rw_ext_anp"
BQ_TABLE_NAME = "multas_aplicadas_acoes_fiscalizacao"

BQ_TABLE = f"{BQ_DATASET}.{BQ_TABLE_NAME}"

COLUMN_MAPPING = {
    # 2016-2019
    "Status Processo": "status_processo",
    "Superintendência": "superintendencia",
    "Número do Processo": "numero_processo",
    "Auto de Infração": "auto_infracao",
    "CNPJ/CPF": "cnpj_cpf",
    "Razão Social": "razao_social",
    "Data Transito Julgado": "data_transito_julgado",
    "Vencimento": "vencimento",
    "Valor da Multa Aplicada": "valor_multa_aplicada",
    "Valor Total Pago (*) (**)": "valor_total_pago",
    # 2020
    " Valor  da Multa Aplicada": "valor_multa_aplicada",
    " Valor Total Pago (*) (**)": "valor_total_pago",
    # 2022
    "Valor da Multa": "valor_multa_aplicada",
    "Valor Total Pago": "valor_total_pago",
    # 2023
    "Superintendcia": "superintendencia",
    "Número do Processo": "numero_processo",
    "Número do DUF": "numero_duf",
    "Razão Social": "razao_social",
    "Vencimento da Multa": "vencimento",
    "Valor da Multa": "valor_multa_aplicada",
    "Valor Total Pago": "valor_total_pago",
    # 2024
    "Número do DUF": "numero_duf",
}

# Arquivos e suas colunas esperadas
FILES_CONFIG = {
    "multasaplicadas2016a2019.csv": {
        "columns": {
            'Status Processo': 'status_processo',
            'Superintendência': 'superintendencia',
            'Número do Processo': 'numero_processo',
            'Auto de Infração': 'auto_infracao',
            'CNPJ/CPF': 'cnpj_cpf',
            'Razão Social': 'razao_social',
            'Data Transito Julgado': 'data_transito_julgado',
            'Vencimento ': 'vencimento',
            'Valor da Multa Aplicada': 'valor_multa_aplicada',
            'Valor Total Pago (*) (**)': 'valor_total_pago'
        },
        "ano_referencia": "2016-2019",
        "header": 4
    },
    "multasaplicadas2020.csv": {
        "columns": {
            'Status Processo': 'status_processo',
            'Superintendência': 'superintendencia',
            'Número do Processo': 'numero_processo',
            'Auto de Infração': 'auto_infracao',
            'CNPJ/CPF': 'cnpj_cpf',
            'Razão Social': 'razao_social',
            'Data Transito Julgado': 'data_transito_julgado',
            'Vencimento': 'vencimento',
            ' Valor  da Multa Aplicada ': 'valor_multa_aplicada',
            ' Valor Total Pago (*) (**)  ': 'valor_total_pago'
        },
        "ano_referencia": "2020",
        "header": 6
    },
    "multasaplicadas2022.csv": {
        "columns": {
            'Status Processo': 'status_processo',
            'Superintendência': 'superintendencia',
            'Número do Processo': 'numero_processo',
            'Número do DUF': 'numero_duf',
            'CNPJ/CPF': 'cnpj_cpf',
            'Razão Social': 'razao_social',
            'Data Transito Julgado': 'data_transito_julgado',
            'Vencimento': 'vencimento',
            'Valor da Multa': 'valor_multa_aplicada',
            'Valor Total Pago': 'valor_total_pago'
        },
        "ano_referencia": "2022",
        "header": 6
    },
    "multasaplicadas2023.csv": {
        "columns": {
            'Status Processo': 'status_processo',
            'Superintendência': 'superintendencia',
            'Número do Processo': 'numero_processo',
            'Número do DUF': 'numero_duf',
            'CNPJ/CPF': 'cnpj_cpf',
            'RazÆo Social': 'razao_social',
            'Data Transito Julgado': 'data_transito_julgado',
            'Vencimento': 'vencimento',
            'Valor da Multa': 'valor_multa_aplicada',
            'Valor Total Pago': 'valor_total_pago'
        },
        "ano_referencia": "2023",
        "encoding": "latin1",
        "header": 5
    },
    "multasaplicadas2024.csv": {
        "columns": {
            'Status Processo': 'status_processo',
            'Superintendência': 'superintendencia',
            'Número do Processo': 'numero_processo',
            'Número do DUF': 'numero_duf',
            'CNPJ/CPF': 'cnpj_cpf',
            'Razão Social': 'razao_social',
            'Data Transito Julgado': 'data_transito_julgado',
            'Vencimento': 'vncimento',
            'Valor da Multa': 'valor_multa_aplicada',
            'Valor Total Pago': 'valor_total_pago'
        },
        "ano_referencia": "2024",
        "header": 6
    }
}

# Schema padronizado final
STANDARDIZED_COLUMNS = [
    'status_processo',
    'superintendencia',
    'numero_processo',
    'auto_infracao',
    'numero_duf',
    'cnpj_cpf',
    'razao_social',
    'data_transito_julgado',
    'vencimento',
    'valor_multa_aplicada',
    'valor_total_pago',
    'ano_referencia',
    'arquivo_origem',
    'data_criacao'
]
