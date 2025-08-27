BASE_URL = (
    "https://www.b3.com.br/pt_br/b3/sustentabilidade/produtos-e-servicos-esg/"
    "credito-de-descarbonizacao-cbio/cbio-consultas/"
)

RAW_DATASET = "rw_ext_anp"

APOSENTADORIA_CBIOS_2023_TABLE = "aposentadoria_cbios_2023"

MAPPING_COLUMNS = {
        'Data': 'data',
        'Quantidade (Parte Obrigada)': 'quantidade_parte_obrigada',
        'Quantidade (Parte Não Obrigada)': 'quantidade_parte_nao_obrigada',
        'Totalização': 'totalizacao'
}
