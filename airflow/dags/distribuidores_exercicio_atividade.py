from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from utils.operators import exec_cloud_run_job, populate_table

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='distribuidores_combustiveis_liquidos_pipeline',
    default_args=default_args,
    description='Pipeline de extração de dados dos distribuidores de combustíveis líquidos da ANP',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
    tags=['anp', 'distribuidores', 'combustiveis-liquidos', 'raw-data'],
) as dag:

    # TaskGroup para ETL dos Distribuidores de Combustíveis Líquidos
    with TaskGroup("etl_distribuidores_comb_liq", tooltip="ETL Distribuidores Combustíveis Líquidos") as etl_distribuidores:

        # Extração dos dados da ANP e inserção no BigQuery (camada raw)
        run_rw_distribuidores_comb_liq = exec_cloud_run_job(
            task_id="raw_distribuidores_comb_liq",
            job_name="cr-juridico-distribuidores-comb-liq-job-dev"
        )

        run_td_distribuidores_comb_liq = populate_table(
            table="rw_anp.distribuidores_combustiveis_liquidos",
            sql_name="/sql/trusted/dml_distribuidores_exercicio_atividade.sql"
        )

        run_rw_distribuidores_comb_liq >> run_td_distribuidores_comb_liq
