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
    dag_id='producao_biodiesel_m3_geral_pipeline',
    default_args=default_args,
    description='Produção de biodiesel m3 geral',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
) as dag:

    run_extraction_producao_biodiesel_m3 = exec_job(
        task_id="extracao_producao_biodiesel_m3_geral",
        job_name="cr-juridico-extracao-producao-biodiesel-m3-geral-job-dev"
    )

    with TaskGroup("etl_producao_biodiesel_m3_geral", tooltip="ETL Produção de biodiesel m3 geral") as etl_producao_biodiesel_m3_geral:
        run_rw_producao_biodiesel_m3 = exec_job(
            task_id="producao_biodiesel_geral_m3",
            job_name="cr-juridico-extracao-producao-biodiesel-m3-geral-job-dev"
        )
        run_rw_producao_biodiesel_m3_geral