from io import BytesIO
import os
import pandas as pd
import requests
import logging
from google.cloud import bigquery
from datetime import date
from constants import (
	URL,
	RAW_DATASET,
	CBIOS_2021_TABLE,
	MAPPING_COLUMNS
)
import logging

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
	level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def rw_ext_anp_cbios_2021():
	"""
	Faz o download de um arquivo Excel contendo dados de CBIOs de 2021, processa e insere os dados em uma tabela particionada do BigQuery.

	"""

	try:
		logging.info("Iniciando o download do arquivo Excel...")
		response = requests.get(URL, verify=False)
		response.raise_for_status()
		file_content = BytesIO(response.content)
		logging.info("Download concluído.")

	except Exception as e:
		logging.warning(f"Erro ao fazer download do arquivo: {e}")
		return None

	df = pd.read_excel(file_content, sheet_name= 'Meta 2021 Publicação', dtype=str)

	df.rename(columns=MAPPING_COLUMNS, inplace=True)

	client = bigquery.Client()
	project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")

	table_id = f"{project_id}.{RAW_DATASET}.{CBIOS_2021_TABLE}"

	job_config = bigquery.LoadJobConfig(
		write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
	)

	partition_key = date.today().strftime('%Y%m%d')

	partitioned_table_id = f"{table_id}${partition_key}"
	logging.info(f"Inserting data for partition: {partition_key}")
	logging.info(f"Total rows to insert: {len(df)}")

	job = client.load_table_from_dataframe(
		df, partitioned_table_id, job_config=job_config
	)
	job.result()
	logging.info(f"Data for {partition_key} inserted successfully.")

	logging.info("Data insertion completed!")

if __name__ == "__main__":
	rw_ext_anp_cbios_2021()