# ETL de contratos de cessão de espaço e carregamento da ANP:
URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vcs/vendas-combustiveis-segmento-m3-2012-2025.csv"
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