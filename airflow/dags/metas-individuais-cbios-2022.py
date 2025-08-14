import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

bucket = os.getenv("BUCKET_NAME", "ext-ecole-biomassa")
project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")

def get_sql_content(sql_path):
    gcs_hook = GCSHook()
    bucket_name, object_name = sql_path.replace('gs://', '').split('/', 1)
    return gcs_hook.download(bucket_name=bucket_name, object_name=object_name).decode('utf-8')

def populate_table(table, sql_name):
    return BigQueryInsertJobOperator(
        task_id=f"populate_query_{table}_job",
        configuration={
            "query": {
                "query": get_sql_content(sql_name),
                "useLegacySql": False
            }
        },
        location="US"
    )

def exec_cloud_run_job(task_id, job_name):
    return CloudRunExecuteJobOperator(
        task_id=f"rw_extract_{task_id}_job",
        job_name=f"cr-juridico-{job_name}-dev",
        region='us-central1',
        project_id=project_id,
        deferrable=True,
        pool="cloud_run_pool",
    )

with DAG(
    dag_id='metas_cbios_2022_pipeline',
    default_args=default_args,
    description='Metas Individuais de CBIOS 2022',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_metas_cbios-2022", tooltip="ETL Metas CBIOS 2022") as etl_metas_cbios_2022:
        run_metas = exec_cloud_run_job(
            task_id="extraction_metas_cbios-2022",
            job_name="extracao-metas-cbios-2022-job"
        )
        run_metas
