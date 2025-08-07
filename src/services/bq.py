import os
from google.cloud import bigquery

def run_query(query, label):
    """
    Executa uma query no BigQuery.

    Args:
        query (str): A query SQL a ser executada.
        label (str): Um rótulo descritivo para a execução da query.
    """
    if label: print(f"Executando query: {label}")
    client = bigquery.Client()
    try:
        job = client.query(query)
        data = job.result()
        print(f"Query executada com sucesso! {label if label else ''}")
        return data
    except Exception as e:
        print(f"Ocorreu um erro ao executar a query: {e}")

def run_query_from_file(file_path, label=None):
    """
    Executa uma query a partir de um arquivo.

    Args:
        file_path (str): Caminho do arquivo contendo a query SQL.
        label (str, optional): Um rótulo descritivo para a execução da query.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    with open(file_path, 'r') as file:
        query = file.read()

    return run_query(query, label)

def get_sql_path(end_path):
    """
    Retorna o caminho completo do arquivo SQL.

    Args:
        end_path (str): Caminho relativo baseado no root do projeto do arquivo SQL.
    """
    base_path = os.getcwd()
    return os.path.join(base_path, end_path)
