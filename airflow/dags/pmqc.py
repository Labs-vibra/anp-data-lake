from airflow import DAG
from utils.operators import exec_cloud_run_job, populate_table
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='pmqc_pipeline',
    default_args=default_args,
    description='PQMC',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_pmqc", tooltip="ETL PMQC") as etl_pmqc:
        run_extracao_pmqc = exec_cloud_run_job(
            task_id="pmqc_extraction",
            job_name="cr-juridico-extracao-pmqc-job-dev"
        )
        # run_rw_pmqc = exec_cloud_run_job(
        # task_id="pmqc_raw",
        # job_name="cr-juridico-rw-pmqc-job-dev"
        # )
        # pop_td_pmqc = populate_table(
        #     table="td_ext_anp.pmqc",
        #     sql_name="/sql/trusted/dml_td_pmqc.sql"
        # )
        run_extracao_pmqc #>> run_rw_pmqc #>>pop_td_pmqc

    etl_pmqc
