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
    dag_id='tancagem_do_abastecimento_nacional_de_combustiveis_pipeline',
    default_args=default_args,
    description='Tancagem do Abastecimento Nacional de Combustíveis',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_tancagem_do_abastecimento_nacional_de_combustiveis",
                   tooltip="ETL Tancagem do Abastecimento Nacional de Combustíveis") as etl_tancagem_do_abastecimento:
        run_rw_tancagem_do_abastecimento_nacional_de_combustiveis = exec_cloud_run_job(
            task_id="tancagem_do_abastecimento_nacional_de_combustiveis_raw",
            job_name="cr-juridico-rw-tancagem-abastecimento-nacional-de-combustiveis-job-dev"
        )
        pop_td_tancagem_do_abastecimento_nacional_de_combustiveis = populate_table(
            table="td_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis",
            sql_name="/sql/trusted/dml_td_tancagem_do_abastecimento_nacional_de_combustiveis.sql"
        )
        run_rw_tancagem_do_abastecimento_nacional_de_combustiveis >> pop_td_tancagem_do_abastecimento_nacional_de_combustiveis

    etl_tancagem_do_abastecimento
