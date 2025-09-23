from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.operators import exec_job, populate_table

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='metas_cbios_2025_pipeline',
    default_args=default_args,
    description='Metas Individuais de CBIOS 2025',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_metas_cbios-2025", tooltip="ETL Metas CBIOS 2025") as etl_metas_cbios_2025:
        run_rw_metas_cbios_2025 = exec_job(
            task_id="extraction_metas_cbios-2025",
            job_name="cr-juridico-extracao-metas-cbios-2025-job-dev"
        )
        pop_td_cbios_2025 = populate_table(
            table="td_ext_anp.cbios_2025",
            sql_name="/sql/trusted/dml_td_cbios_2025.sql"
        )
        run_rw_metas_cbios_2025 >> pop_td_cbios_2025
