BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2024/metas-definitiva"
	"s-2024.xlsx/@@download/file"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2024_TABLE = "cbios_2024"

MAPPING_COLUMNS = {
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ': 'cnpj',
		'Razão Social': 'razao_social',
		'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
		'Participação \nde Mercado (%)': 'participacao_mercado',
		'Meta Definitiva 2024': 'meta_definitiva_2024',
}