import os
import sys
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

SCHEMAS_PATH = "./db/schemas"
REGION = "us-central1"

schema_files = [
    # RAW Layer
    "db/schemas/raw/ddl_consulta_bases_de_distribuicao_e_trr_autorizados.sql",
    "db/schemas/raw/ddl_postos_revendedores.sql",

    
    # TRUSTED Layer
    "db/schemas/trusted/ddl_consulta_bases_de_distribuicao_e_trr_autorizados.sql",
    "db/schemas/trusted/ddl_postos_revendedores.sql",
]

datasets_files = []
for root, dirs, files in os.walk(SCHEMAS_PATH):
    for f in files:
        if os.path.isfile(os.path.join(root, f)) and f.endswith('.sql'):
            if os.path.basename(f).startswith('ddl_datasets'):
                datasets_files.append(os.path.join(root, f))
#            else:
#                schema_files.append(os.path.join(root, f))


def execute_sql_files(files, category_name):
    """
    Execute a list of SQL files in BigQuery.

    Args:
        files (list): List of file paths to execute
        category_name (str): Name of the category for logging purposes
    """
    client = bigquery.Client()
    print(f"\n=== {category_name} ===")

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql = f.read()
                print(f"Executing SQL from {file_path}...")
                query_job = client.query(sql)
                query_job.result()  # Waits for the job to complete.
                print(f"✅ Executed {file_path} successfully.")
        except Exception as e:
            print(f"❌ Error executing {file_path}: {str(e)}")

# Execute datasets and tables
if len(sys.argv) > 1:
    # If a file path is provided as argument, execute only that file
    file_path = sys.argv[1]
    if os.path.isfile(file_path) and file_path.endswith('.sql'):
        execute_sql_files([file_path], f"Executing {os.path.basename(file_path)}")
    else:
        print(f"❌ Error: '{file_path}' is not a valid SQL file")
        sys.exit(1)
else:
    print(f"Found {len(schema_files)} SQL files to execute (excluding datasets):")
    for file in schema_files:
        print(f"  - {file}")

    print(f"Found {len(datasets_files)} dataset SQL files:")
    for file in datasets_files:
        print(f"  - {file}")
    execute_sql_files(datasets_files, "Creating Datasets")
    execute_sql_files(schema_files, "Creating Tables")
    print("\n🎉 Database deployment completed!")
