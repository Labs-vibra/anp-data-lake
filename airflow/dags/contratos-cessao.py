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
    dag_id='contratos_cessao_pipeline',
    default_args=default_args,
    description='Pipeline para Contratos de Cessão',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_contratos_cessao", tooltip="ETL Contratos Cessão") as etl_contratos_cessao:
        run_rw_contratos_cessao = exec_cloud_run_job(
            task_id="extraction_contratos_cessao",
            job_name="cr-juridico-extracao-contratos-cessao-job-dev"
        )
        run_rw_contratos_cessao
