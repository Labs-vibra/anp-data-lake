BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2019/metas-individ"
	"uais-compulsorias-2019.xlsx"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2019_TABLE = "cbios_2019"

MAPPING_COLUMNS = {
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ': 'cnpj',
		'Razão Social': 'razao_social',
		'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
		'Participação \nde Mercado (%)': 'participacao_mercado',
		'Meta Individual 2019\n(CBIO)': 'meta_individual_2019',
		'(8/365) * \n(Meta Individual 2019)\n(CBIO)': 'meta_individual_2019_diaria',
}