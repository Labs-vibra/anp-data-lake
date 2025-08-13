BUCKET_NAME="ext-ecole-biomassa"

# ETL de metas individuais de CBIOS de 2022:
URL = (
    "https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2022/meta_individual_publicacao_2022.xlsx"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2022_TABLE = "cbios_2022"

MAPPING_COLUMNS = {
	'Código do\nAgente Regulado': 'codigo_agente_regulado',
	'CNPJ': 'cnpj',
	'Razão Social': 'razao_social',
	'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
	'Participação \nde Mercado (%)': 'participacao_mercado',
	'Meta Individual  2022\n(CBIO)': 'meta_individual_2022',
}