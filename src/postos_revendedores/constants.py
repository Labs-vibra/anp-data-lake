import os

# Configurações do BigQuery
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")
BQ_DATASET = "rw_ext_anp"
TABLE_NAME = "postos_revendedores"

# Configurações do Cloud Storage
BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
BUCKET_PATH = "anp/postos_revendedores/"

# URL do arquivo CSV da ANP
ANP_URL = "https://cdp.anp.gov.br/ords/r/cdp_apex/consulta-dados-publicos-cdp/consulta-de-postos-lista"

COLUMNS = {
	"Nº Autorizacao": "numero_autorizacao",
	"Data Publicação DOU - Autorização": "data_publicacao_dou_autorizacao",
	"Código Instalação i-Simp": "codigo_instalacao_i_simp",
	"Razão Social": "razao_social",
	"CNPJ": "cnpj",
	"Endereço": "endereco",
	"COMPLEMENTO": "complemento",
	"BAIRRO": "bairro",
	"CEP": "cep",
	"UF": "uf",
	"MUNICÍPIO": "municipio",
	"Vinculação a Distribuidor": "vinculacao_a_distribuidor",
	"Data de Vinculação a Distribuidor": "data_de_vinculacao_a_distribuidor",
	"Produto": "produto",
	"Tancagem (m³)": "tancagem_m3",
	"Qtde de Bico": "qtde_de_bico",
	"LATITUDE": "latitude",
	"LONGITUDE": "longitude",
	"Delivery": "delivery",
	"Data Autorização Delivery": "data_autorizacao_delivery",
	"Número Despacho Delivery": "numero_despacho_delivery",
	"Status PMQC": "status_pmqc"
}