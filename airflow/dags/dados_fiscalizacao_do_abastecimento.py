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
    dag_id='dados_fiscalizacao_do_abastecimento_pipeline',
    default_args=default_args,
    description='Dados Fiscalização do Abastecimento',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_dados_fiscalizacao_do_abastecimento",
                   tooltip="ETL Dados Fiscalização do Abastecimento") as etl_dados_fiscalizacao_do_abastecimento:
        run_rw_dados_fiscalizacao_do_abastecimento = exec_cloud_run_job(
            task_id="dados_fiscalizacao_do_abastecimento_raw",
            job_name="cr-juridico-rw-dados-fiscalizacao-do-abastecimento-job-dev"
        )
        pop_td_dados_fiscalizacao_do_abastecimento = populate_table(
            table="td_ext_anp.dados_fiscalizacao_do_abastecimento",
            sql_name="/sql/trusted/dml_td_dados_fiscalizacao_do_abastecimento.sql"
        )
        run_rw_dados_fiscalizacao_do_abastecimento >> pop_td_dados_fiscalizacao_do_abastecimento
    etl_dados_fiscalizacao_do_abastecimento
