import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2020/metas-individ"
	"uais-compulsorias-2020.xlsx"
)

RAW_DATASET = "rw_ext_anp"

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")

print(f"Using PROJECT_ID: {PROJECT_ID}")


CBIOS_2020_TABLE = "cbios_2020"

MAPPING_COLUMNS = {
    'Razão Social': 'razao_social',
    'Código do\nAgente Regulado': 'codigo_agente_regulado',
    'CNPJ': 'cnpj',
    'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
    'Participação \nde Mercado (%)': 'participacao_mercado',
    'Meta Individual 2020\n(CBIO)': 'meta_individual_2020',
}
