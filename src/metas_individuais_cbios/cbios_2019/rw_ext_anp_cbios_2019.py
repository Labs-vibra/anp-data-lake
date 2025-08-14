import os
import requests
import logging
import pandas as pd
from io import BytesIO
from google.cloud import bigquery
from datetime import date
from constants import (
	BASE_URL,
	RAW_DATASET,
	CBIOS_2019_TABLE,
	MAPPING_COLUMNS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def rw_ext_anp_cbios_2019():
	"""
	Realiza a extração de arquivos de logística da ANP:
    - Baixa o XLSX direto do link de download
	- Normaliza as colunas para STRING
    - Padroniza o nome das colunas
	- Envia os dados para o BigQuery
	"""

	logging.info("Iniciando o download do arquivo Excel...")
	response = requests.get(BASE_URL, verify=False)
	response.raise_for_status()
	file_content = BytesIO(response.content)
	logging.info("Download concluído.")

	df = pd.read_excel(file_content, dtype=str)
	df = df.iloc[:-2]

	df.rename(columns=MAPPING_COLUMNS, inplace=True)

	client = bigquery.Client()
	project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")

	table_id = f"{project_id}.{RAW_DATASET}.{CBIOS_2019_TABLE}"

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
	rw_ext_anp_cbios_2019()
