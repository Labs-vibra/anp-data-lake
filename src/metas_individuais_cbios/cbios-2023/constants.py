BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2023/planilha-meta-"
	"definitiva-2023.xlsx/@@download/file"
)

RAW_DATASET = "rw_ext_anp"

CBIOS_2023_TABLE = "cbios_2023"

MAPPING_COLUMNS = {
	'Razão Social': 'razao_social', 
	'CNPJ': 'cnpj', 
	'Meta CNPE 2023 individualizada (CBIOs)': 'meta_cnpe_2023_individualizada_cbios',
	'Meta individual 2022 não cumprida (CBIOs)': 'meta_individual_2022_nao_cumprida_cbios',
	' Meta individual 2023 a ser cumprida até 31/3/2024 (CBIOs)': 'meta_individual_2023_a_ser_cumprida_ate_20240331_cbios'
}