from io import BytesIO
import os
from urllib import response
import pandas as pd
import requests
from google.cloud import bigquery, storage
from datetime import date
from constants import (
    URL,
    MAPPING_COLUMNS
)
import logging


logging.basicConfig(level=logging.INFO)

bucket_name = os.getenv("GCP_BUCKET_NAME")
dest_path = "raw"

def rw_ext_anp_cbios_2022():
	try:
		logging.info("Iniciando o download do arquivo Excel...")
		response = requests.get(URL, verify=False)
		response.raise_for_status()
		file_content = BytesIO(response.content)
		logging.info("Download concluído.")

	except Exception as e:
		logging.warning(f"Erro ao fazer download do arquivo: {e}")
		return None
	

	df = pd.read_excel(file_content, sheet_name= 'Meta CNPE 2022 ')
	print(df.columns)

	df.rename(columns=MAPPING_COLUMNS, inplace=True)

	# client = bigquery.Client()
	# project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
	# bq_dataset = "rw_ext_anp"
	# table_name = "cbios_2019"

	# table_id = f"{project_id}.{bq_dataset}.{table_name}"

	# job_config = bigquery.LoadJobConfig(
	# 	write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
	# )

	# partition_key = date.today().strftime('%Y%m%d')

	# partitioned_table_id = f"{table_id}${partition_key}"
	# print(f"Inserting data for partition: {partition_key}")
	# print(f"Total rows to insert: {len(df)}")

	# job = client.load_table_from_dataframe(
	# 	df, partitioned_table_id, job_config=job_config
	# )
	# job.result()
	# print(f"Data for {partition_key} inserted successfully.")

	# print("Data insertion completed!")

if __name__ == "__main__":
	rw_ext_anp_cbios_2022()
