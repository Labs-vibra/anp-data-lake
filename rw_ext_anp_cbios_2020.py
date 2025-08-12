from src.utils.constants import PATHS
from src.utils.utils import download_file
import os
import pandas as pd
#from google.cloud import bigquery, storage
from datetime import date

URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2020/metas-individuais-compulsorias-2020.xlsx"
)

bucket_name = os.getenv("GCP_BUCKET_NAME")
dest_path = "raw"

def rw_ext_anp_cbios_2020():
	try:
		file_content = download_file(URL)
		filename = "metas-individuais-compulsorias-2020.xlsx"
		local_file_path = os.path.join(PATHS["RAW_DIR"], filename)

		os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
		with open(local_file_path, 'wb') as f:
			f.write(file_content.read())
		
		print(f"Arquivo salvo em: {local_file_path}")

		df = pd.read_excel(local_file_path, sheet_name=1)
		test = df.rename(columns={
			'Razão Social': 'razao_social',
			'Código do\nAgente Regulado': 'codigo_agente_regulado',
			'CNPJ': 'cnpj',
			'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_das_emissoes', 
			'Participação \nde Mercado (%)': 'participacao_de_mercado',
			'Meta Individual 2020\n(CBIO)': 'meta_individual_2020',
		})

		return test
    
	except Exception as e:
		print(f"Erro ao baixar o arquivo: {e}")
		return None
