import os
import pandas as pd
from io import BytesIO
from google.cloud import bigquery, storage
from datetime import date
import requests

BASE_URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2019/metas-individ"
	"uais-compulsorias-2019.xlsx"
)

def rw_ext_anp_cbios_2019():
	try:
		response = requests.get(BASE_URL, verify=False)
		response.raise_for_status()
		file_content = BytesIO(response.content)
		filename = "metas-individuais-compulsorias-2019.xlsx"

		bucket_name = os.getenv("GCP_BUCKET_NAME")
		dest_path = f"anp/{filename}"
		project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
		client = storage.Client(project=project_id)
		bucket = client.bucket(bucket_name)
		blob = bucket.blob(dest_path)
		file_content.seek(0)
		blob.upload_from_file(file_content)
		print(f"Arquivo enviado para: gs://{bucket_name}/{dest_path}")

	except Exception as e:
		print(f"Erro ao fazer upload do arquivo: {e}")
		return None

	df = pd.read_excel(file_content)

	df.rename(columns={
		'Código do\nAgente Regulado': 'codigo_agente_regulado',
		'CNPJ': 'cnpj',
		'Razão Social': 'razao_social',
		'Somatório das Emissões \n(tCO2 equivalente)': 'somatorio_emissoes',
		'Participação \nde Mercado (%)': 'participacao_mercado',
		'Meta Individual 2019\n(CBIO)': 'meta_individual_2019',
		'(8/365) * \n(Meta Individual 2019)\n(CBIO)': 'meta_individual_2019_diaria',
	}, inplace=True)

	df['codigo_agente_regulado'] = df['codigo_agente_regulado'].astype(str)

	client = bigquery.Client()
	project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")
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

if __name__ == "__main__":
	rw_ext_anp_cbios_2019()