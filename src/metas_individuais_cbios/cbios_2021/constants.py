
# ETL de Metas Individuais de Cbios 2021:
URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2021/metaindividual"
	"2021-21jul.xlsx"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2021_TABLE = "cbios_2021"

MAPPING_COLUMNS = {
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ': 'cnpj',
		'Razão Social': 'razao_social',
		'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
		'Participação \nde Mercado (%)': 'participacao_mercado',
		'Nova Meta Individual  2021\n(CBIO)': 'meta_individual_2021' 
}