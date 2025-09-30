# ETL de vendas de cmobustíveis por segmento:

URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis"
)

TABLE_ID = "raw_ext_anp.vendas_combustiveis_segmento"

MAPPING_COLUMNS = {
    'ANO': 'ano',
    'MÊS': 'mes',
    'UNIDADE DA FEDERAÇÃO': 'unidade_da_federacao',
    'PRODUTO': 'produto',
    'SEGMENTO': 'segmento',
    'VENDAS': 'vendas',
}