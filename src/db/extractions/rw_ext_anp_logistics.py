import os
import zipfile
import pandas as pd
from google.cloud import bigquery

# # Configurações gerais
# ZIP_PATH = 'caminho/para/seu_arquivo.zip'  # Pode ser alterado para input ou argumento
# EXTRACT_DIR = 'temp_extracted'
# PROJECT_ID = 'seu-projeto-gcp'
# DATASET_ID = 'raw_logistics'
# TABLES = {
#     'logistica01.csv': 'logistica_01',
#     'logistica02.csv': 'logistica_02',
#     'logistica03.csv': 'logistica_03'
# }

def extract_zip(zip_path: str, extract_to: str):
    """Extrai arquivos do zip para uma pasta temporária."""
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f'Arquivos extraídos para {extract_to}')

def load_csv_to_bq(file_path: str, table_id: str, client: bigquery.Client):
    """Carrega um CSV para uma tabela BigQuery"""
    df = pd.read_csv(file_path)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # substitui dados da tabela
        autodetect=True,  # detecta schema automaticamente (ideal para teste inicial)
        source_format=bigquery.SourceFormat.CSV,
    )
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # espera terminar
    print(f'Dados carregados para {table_ref}, {df.shape[0]} linhas.')

def main():
    client = bigquery.Client(project=PROJECT_ID)
    extract_zip(ZIP_PATH, EXTRACT_DIR)
    for filename, table_name in TABLES.items():
        csv_path = os.path.join(EXTRACT_DIR, filename)
        if os.path.exists(csv_path):
            load_csv_to_bq(csv_path, table_name, client)
        else:
            print(f'Arquivo {filename} não encontrado no zip.')
    # Limpeza opcional dos arquivos temporários
    # shutil.rmtree(EXTRACT_DIR)

if __name__ == '__main__':
    main()

