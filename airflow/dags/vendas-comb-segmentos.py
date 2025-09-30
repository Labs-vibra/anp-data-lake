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
    dag_id='vendas_comb_segmento_pipeline',
    default_args=default_args,
    description='Pipeline para Vendas de Combustíveis por Segmento',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_vendas_comb_segmento", tooltip="ETL Vendas Combustíveis por Segmento") as etl_vendas_comb_segmento:
        run_rw_vendas_comb_segmento = exec_cloud_run_job(
            task_id="extraction_vendas_comb_segmento",
            job_name="cr-juridico-extracao-vendas-comb-segmento-job-dev"
        )
        run_rw_vendas_comb_segmento
