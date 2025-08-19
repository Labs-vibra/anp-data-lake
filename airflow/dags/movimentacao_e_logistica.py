from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.operators import exec_cloud_run_job, populate_table
import datetime as dt

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='logistics_pipeline',
    default_args=default_args,
    description='Movimentação e Logística DAG',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    run_extract_logistics = exec_cloud_run_job(
        task_id="extract_logistics_files",
        job_name="cr-juridico-extracao-logistica-job-dev"
    )

    with TaskGroup("etl_logistics_01", tooltip="ETL Logística 01") as etl_logistics_01:
        run_rw_logistics_01 = exec_cloud_run_job(
            task_id="logistics_01",
            job_name="cr-juridico-extracao-logistica-01-job-dev"
        )
        pop_td_logistics_01 = populate_table(
            table="td_ext_anp.logistics_01",
            sql_name=f"/sql/trusted/dml_td_logistics_01.sql"
        )
        run_rw_logistics_01 >> pop_td_logistics_01

    with TaskGroup("etl_logistics_02", tooltip="ETL Logística 02") as etl_logistics_02:
        run_rw_logistics_02 = exec_cloud_run_job(
            task_id="logistics_02",
            job_name="cr-juridico-extracao-logistica-02-job-dev"
        )
        pop_td_logistics_02 = populate_table(
            table="td_ext_anp.logistics_02",
            sql_name=f"gs://{bucket}/sql/trusted/dml_td_logistics_02.sql"
        )
        run_rw_logistics_02 >> pop_td_logistics_02

    run_extract_logistics >> [etl_logistics_01, etl_logistics_02]


