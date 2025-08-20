from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.operators import exec_cloud_run_job, populate_table

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='metas_cbios_2024_pipeline',
    default_args=default_args,
    description='Metas Individuais de CBIOS 2024',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_metas_cbios-2024", tooltip="ETL Metas CBIOS 2024") as etl_metas_cbios_2024:
        run_metas = exec_cloud_run_job(
            task_id="extraction_metas_cbios-2024",
            job_name="cr-juridico-extracao-metas-cbios-2024-job-dev"
        )
        pop_td_cbios_2024 = populate_table(
            table="td_ext_anp.cbios_2024",
            sql_name=f"/sql/trusted/dml_td_cbios_2024.sql"
        )
        run_metas >> pop_td_cbios_2024