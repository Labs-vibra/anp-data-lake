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
    "Nσero do Processo": "numero_processo",
    "Nσero do DUF": "numero_duf",
    "Raz釅 Social": "razao_social",
    "Vencimento da Multa": "vencimento",
    " Valor da Multa": "valor_multa_aplicada",
    " Valor Total Pago": "valor_total_pago",
    # 2024
    "Número do DUF": "numero_duf",
}

# Arquivos e suas colunas esperadas
FILES_CONFIG = {
    "multasaplicadas2016a2019.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Auto de Infração", "CNPJ/CPF", "Razão Social",
            "Data Transito Julgado", "Vencimento",
            "Valor da Multa Aplicada", "Valor Total Pago (*) (**)"
        ],
        "ano_referencia": "2016-2019"
    },
    "multasaplicadas2020.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Auto de Infração", "CNPJ/CPF", "Razão Social",
            "Data Transito Julgado", "Vencimento",
            " Valor  da Multa Aplicada", " Valor Total Pago (*) (**)"
        ],
        "ano_referencia": "2020"
    },
    "multasaplicadas2022.csv": {
        "columns": [
            "Razão Social", "Data Transito Julgado", "Vencimento",
            "Valor da Multa", "Valor Total Pago"
        ],
        "ano_referencia": "2022"
    },
    "multasaplicadas2023.csv": {
        "columns": [
            "Status Processo", "Superintendcia", "Nσero do Processo",
            "Nσero do DUF", "CNPJ/CPF", "Raz釅 Social",
            "Vencimento da Multa", " Valor da Multa", " Valor Total Pago"
        ],
        "ano_referencia": "2023"
    },
    "multasaplicadas2024.csv": {
        "columns": [
            "Status Processo", "Superintendência", "Número do Processo",
            "Número do DUF", "CNPJ/CPF", "Razão Social",
            "Vencimento", "Valor da Multa", "Valor Total Pago"
        ],
        "ano_referencia": "2024"
    }
}

# Schema padronizado final
STANDARDIZED_COLUMNS = [
    "status_processo",
    "superintendencia",
    "numero_processo",
    "auto_infracao",
    "numero_duf",
    "cnpj_cpf",
    "razao_social",
    "data_transito_julgado",
    "vencimento",
    "valor_multa_aplicada",
    "valor_total_pago",
    "ano_referencia",
    "arquivo_origem"
]
