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
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_producao_biodiesel_m3_geral", tooltip="ETL Produção de biodiesel m3 geral") as etl_producao_biodiesel_m3_geral:
        pop_td_producao_biodiesel_m3_geral = populate_table(
            table="td_ext_anp.producao_biodiesel_m3_geral",
            sql_name=f"/sql/trusted/dml_td_producao_biodiesel_m3_geral.sql"
        )

        pop_td_producao_biodiesel_m3_geral