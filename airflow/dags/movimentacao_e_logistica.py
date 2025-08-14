from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import datetime as dt

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

bucket = os.getenv("BUCKET_NAME", "ext-ecole-biomassa")
project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa-468317")

# params_dag = {
#     'start_date': '2023-01-01',
#     'end_date': '2023-12-31',
# }

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
        # params=params_dag,
        location="US"
    )

def exec_cloud_run_job(task_id, job_name):
    return CloudRunExecuteJobOperator(
        task_id=f"rw_extract_{task_id}_job",
        job_name=f"cr-juridico-{job_name}-dev",
        region='us-central1',
        project_id=project_id,
        deferrable=True,
        # overrides={
        #     "container_overrides": [
        #         {
        #             "env": [
        #                 {"name": "START_DATE", "value": params_dag['start_date']},
        #                 {"name": "END_DATE", "value": params_dag['end_date']}
        #             ]
        #         }
        #     ],
        # },
        pool="cloud_run_pool",
    )

with DAG(
    dag_id='logistics_pipeline',
    default_args=default_args,
    description='Movimentação e Logística DAG',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_logistics", tooltip="ETL Logistics") as etl_logistics:
        run_logistics = exec_cloud_run_job(
            task_id="extraction_logistics",
            job_name="etl-logistics"
        )

    with TaskGroup("rw_ext_anp_logistics_02", tooltip="Raw ETL Logística 02") as rw_logistics_02:
        run_rw_logistics_02 = exec_cloud_run_job(
            task_id="logistics_02",
            job_name="etl-logistics-02"
        )

    run_logistics >> [run_rw_logistics_02]
