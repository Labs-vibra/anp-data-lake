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
    dag_id='codigos_instalacao_pipeline',
    default_args=default_args,
    description='Códigos de Instalação Pipeline',
    schedule_interval='None',
    catchup=False,
    max_active_tasks=2,
) as dag:

    run_extraction_manual_simp = exec_cloud_run_job(
        task_id="extracao_manual_simp",
        job_name="cr-juridico-extracao-manual-simp-job-dev"
    )

    with TaskGroup("etl_codigos_instalacao", tooltip="ETL Códigos de Instalação") as etl_codigos_instalacao:
        run_rw_codigos_instalacao = exec_cloud_run_job(
            task_id="codigos_instalacao",
            job_name="cr-juridico-extracao-codigos-instalacao-job-dev"
        )
        run_rw_codigos_instalacao