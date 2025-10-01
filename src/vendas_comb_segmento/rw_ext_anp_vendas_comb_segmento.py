import os
import requests
import logging
import pandas as pd
from io import BytesIO
from google.cloud import bigquery
from datetime import date
from bs4 import BeautifulSoup
from constants import (
	URL,
	TABLE_ID,
	MAPPING_COLUMNS
)

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_download_link(url):
	"""
	Obtém o link de download do arquivo CSV a partir da página HTML.
	"""
	logging.info("Buscando o link de download na página...")
	response = requests.get(url, verify=False)
	response.raise_for_status()
	soup = BeautifulSoup(response.content, "html.parser")

	# Encontra o elemento que contém "Vendas de combustíveis por segmento"
	target_text = "Vendas de combustíveis por segmento"
	target_element = soup.find_all("a", string=lambda text: text and target_text.lower() in text.lower())
	
	if not target_element:
		raise ValueError(f"Elemento com texto '{target_text}' não encontrado na página.")

	for links in target_element:
		href = links.get("href", "")
		if href.lower().endswith(".csv"):
			csv_url = href

	# raise ValueError(f"Nenhum link CSV encontrado para '{target_text}'.")
	if not csv_url:
		raise ValueError(f"Nenhum CSV encontrado para '{target_text}'.")
	

	return csv_url if csv_url else None


def rw_ext_anp_vendas_comb_segmento(csv_url):
	"""
	Realiza a extração de arquivo de vendas de combustíveis por segmento do site da ANP:
	- Baixa o XLSX direto do link de download
	- Normaliza as colunas para STRING
	- Padroniza o nome das colunas
	- Envia os dados para o BigQuery
	"""

	logging.info("Iniciando o download do arquivo...")
	response = requests.get(csv_url, verify=False)
	response.raise_for_status()
	file_content = BytesIO(response.content)
	logging.info("Download concluído.")

	df = pd.read_csv(file_content, sep=';', dtype=str)

	df.rename(columns=MAPPING_COLUMNS, inplace=True)

	client = bigquery.Client()
	project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")

	table_id = f"{project_id}.{TABLE_ID}"

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
	csv_url = get_download_link(URL)
	rw_ext_anp_vendas_comb_segmento(csv_url)