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

# bucket = os.getenv("BUCKET_NAME", "vibra-dtan-juridico-anp-input")
# project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "labs-vibra-final")

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

# with DAG(
#     dag_id='logistics_pipeline',
#     default_args=default_args,
#     description='Movimentação e Logística DAG',
#     schedule_interval='@monthly',
#     catchup=False,
#     max_active_tasks=2,
# ) as dag:

#     with TaskGroup("etl_logistics", tooltip="ETL Logistics") as etl_logistics:
#         run_logistics = exec_cloud_run_job(
#             task_id="raw_logistics",
#             job_name="etl-logistics"
#         )
#         pop_logistics = populate_table(
#             table="td_logistics_01",
#             sql_name=f"gs://{bucket}/sql/trusted/dml_logistics.sql"
#         )
#         run_logistics >> pop_logistics
