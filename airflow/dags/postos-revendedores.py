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
    dag_id='postos_revendedores_pipeline',
    default_args=default_args,
    description='Postos Revendedores',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_postos_revendedores", tooltip="ETL Postos Revendedores") as etl_postos_revendedores:
        run_rw_postos_revendedores = exec_cloud_run_job(
            task_id="postos_revendedores_raw",
            job_name="cr-juridico-rw-postos-revendedores-job-dev"
        )
        pop_td_postos_revendedores = populate_table(
            table="td_ext_anp.postos_revendedores",
            sql_name="/sql/trusted/dml_td_postos_revendedores.sql"
        )
        run_rw_postos_revendedores >> pop_td_postos_revendedores

    etl_postos_revendedores
