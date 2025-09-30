from airflow import DAG
from utils.operators import exec_cloud_run_job, populate_table
from airflow.utils.dates import days_ago # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='consulta_bases_de_distribuicao_e_trr_autorizados_pipeline',
    default_args=default_args,
    description='Consulta das Bases de Distribuição e de TRR Autorizados',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_consulta_bases_de_distribuicao_e_trr_autorizados",
                   tooltip="ETL Tancagem do Abastecimento Nacional de Combustíveis") as etl_consulta_bases_de_distribuicao_e_trr_autorizado:
        run_rw_consulta_bases_de_distribuicao_e_trr_autorizados = exec_cloud_run_job(
            task_id="consulta_bases_de_distribuicao_e_trr_autorizados_raw",
            job_name="cr-juridico-rw-consulta-bases-de-distribuicao-e-trr-autorizados-job-dev"
        )
        run_rw_consulta_bases_de_distribuicao_e_trr_autorizados

    etl_consulta_bases_de_distribuicao_e_trr_autorizado
