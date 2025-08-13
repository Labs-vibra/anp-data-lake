import os
import pandas as pd
from google.cloud import bigquery, storage
from datetime import date
from services.constants import PATHS
from services.utils import download_file


URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2019/metas-individuais-compulsorias-2019.xlsx"
)

bucket_name = os.getenv("GCP_BUCKET_NAME")
dest_path = "raw"

def rw_ext_anp_cbios_2019():
	try:
		file_content = download_file(URL)
		filename = "metas-individuais-compulsorias-2019.xlsx"
		local_file_path = os.path.join(PATHS["RAW_DIR"], filename)

		os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
		with open(local_file_path, 'wb') as f:
			f.write(file_content.read())
		
		print(f"Arquivo salvo em: {local_file_path}")

	except Exception as e:
		print(f"Erro ao baixar o arquivo: {e}")
		return None

	df = pd.read_excel(local_file_path)
	print(df.columns)
	df.rename(columns={
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ': 'cnpj',
		'Razão Social': 'razao_social',
		'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
		'Participação \nde Mercado (%)': 'participacao_mercado',
		'Meta Individual 2019\n(CBIO)': 'meta_individual_2019',
		'(8/365) * \n(Meta Individual 2019)\n(CBIO)': 'meta_individual_2019_diaria',
	})

	client = bigquery.Client()
	project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
	bq_dataset = "rw_ext_anp"
	table_name = "cbios_2019"

	table_id = f"{project_id}.{bq_dataset}.{table_name}"

	job_config = bigquery.LoadJobConfig(
		write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
	)

	partition_key = date.today().strftime('%Y%m%d')

	partitioned_table_id = f"{table_id}${partition_key}"
	print(f"Inserting data for partition: {partition_key}")
	print(f"Total rows to insert: {len(df)}")

	job = client.load_table_from_dataframe(
		df, partitioned_table_id, job_config=job_config
	)
	job.result()
	print(f"Data for {partition_key} inserted successfully.")

	print("Data insertion completed!")


