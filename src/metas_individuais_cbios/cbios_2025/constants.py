BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2025/meta-definitiva."
	"xlsx/@@download/file"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2025_TABLE = "cbios_2025"

MAPPING_COLUMNS = {
		'CNPJ': 'cnpj',
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ.1': 'cnpj_1',
		'Razão Social': 'razao_social',
		'Meta CNPE 2025 individualizada (CBIO)': 'meta_cnpe_2025_individualizada_cbio',
		'CBIOs a Abater em decorrência de contratos de longo prazo com vigência terminada em 2024': 'cbios_a_abater_2024',
		'Meta individual não cumprida até 31/12/2024 (CBIO)': 'meta_individual_nao_cumprida_2024',
		'Meta individual 2025 a ser cumprida até 31/12/2025 (CBIO)': 'meta_individual_2025_a_ser_cumprida_2025'
}

DEV_PROJECT_ID = "ext-ecole-biomassa-468317"