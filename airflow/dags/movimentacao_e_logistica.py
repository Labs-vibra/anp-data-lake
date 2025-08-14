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
    dag_id='logistics_pipeline',
    default_args=default_args,
    description='Movimentação e Logística DAG',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("extract_ext_anp_logistics", tooltip="Extração de arquivos de Logística") as tg_extract_logistics:
        run_extract_logistics = exec_cloud_run_job(
            task_id="extract_logistics_files",
            job_name="etl-logistics-extraction"
        )

    with TaskGroup("etl_logistics_01", tooltip="ETL Logística 01") as etl_logistics_01:
        run_rw_logistics_01 = exec_cloud_run_job(
            task_id="logistics_01",
            job_name="etl-logistics-01"
        )
        pop_td_logistics_01 = populate_table(
            table="td_ext_anp.logistics_01",
            sql_name=f"gs://{bucket}/sql/trusted/dml_td_logistics_01.sql"
        )
        # pop_rf_logistics_01 = populate_table(
        #     table="rf_ext_anp.logistics_01",
        #     sql_name=f"gs://{bucket}/sql/refined/dml_rf_logistics_01.sql"
        # )
        run_rw_logistics_01 >> pop_td_logistics_01 #>> pop_rf_logistics_01

    with TaskGroup("etl_logistics_02", tooltip="ETL Logística 02") as etl_logistics_02:
        run_rw_logistics_02 = exec_cloud_run_job(
            task_id="logistics_02",
            job_name="etl-logistics-02"
        )
        pop_td_logistics_02 = populate_table(
            table="td_ext_anp.logistics_02",
            sql_name=f"gs://{bucket}/sql/trusted/dml_td_logistics_02.sql"
        )
        # pop_rf_logistics_02 = populate_table(
        #     table="rf_ext_anp.logistics_02",
        #     sql_name=f"gs://{bucket}/sql/refined/dml_rf_logistics_02.sql"
        # )
        run_rw_logistics_02 >> pop_td_logistics_02 #>> pop_rf_logistics_02

    tg_extract_logistics >> [etl_logistics_01, etl_logistics_02]


