import requests
import logging
import pandas as pd
from io import BytesIO
from google.cloud import bigquery
from datetime import date
from constants import (
	BASE_URL,
	RAW_DATASET,
	CBIOS_2020_TABLE,
	MAPPING_COLUMNS,
    PROJECT_ID
)

logging.basicConfig(level=logging.INFO)

def rw_ext_anp_cbios_2020():
	try:
		logging.info("Iniciando o download do arquivo Excel...")
		response = requests.get(BASE_URL, verify=False)
		response.raise_for_status()
		file_content = BytesIO(response.content)
		logging.info("Download concluído.")
	except Exception as e:
		logging.warning(f"Erro ao fazer download do arquivo: {e}")
		return None

	df = pd.read_excel(file_content, sheet_name=1, dtype=str)
	df = df.iloc[:-2]
	print(df.columns)

	# df.rename(columns=MAPPING_COLUMNS, inplace=True)
	# client = bigquery.Client()

	# table_id = f"{PROJECT_ID}.{RAW_DATASET}.{CBIOS_2020_TABLE}"

	# job_config = bigquery.LoadJobConfig(
	# 	write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
	# )
	# partition_key = date.today().strftime('%Y%m%d')

	# partitioned_table_id = f"{table_id}${partition_key}"
	# logging.info(f"Inserting data for partition: {partition_key}")
	# logging.info(f"Total rows to insert: {len(df)}")

	# job = client.load_table_from_dataframe(
	# 	df, partitioned_table_id, job_config=job_config
	# )
	# job.result()
	# logging.info(f"Data for {partition_key} inserted successfully.")
	# logging.info("Data insertion completed!")

if __name__ == "__main__":
	rw_ext_anp_cbios_2020()
