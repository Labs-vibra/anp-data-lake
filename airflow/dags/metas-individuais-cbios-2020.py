from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.operators import exec_cloud_run_job

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='metas_cbios_2020_pipeline',
    default_args=default_args,
    description='Metas Individuais de CBIOS 2020',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_metas_cbios-2020", tooltip="ETL Metas CBIOS 2020") as etl_metas_cbios_2020:
        run_metas = exec_cloud_run_job(
            task_id="extraction_metas_cbios-2020",
            job_name="cr-juridico-extracao-metas-cbios-2020-job-dev"
        )
        run_metas
