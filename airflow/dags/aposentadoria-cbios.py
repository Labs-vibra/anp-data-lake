from airflow import DAG
from utils.operators import populate_table, exec_job
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='aposentadoria_cbios_pipeline',
    default_args=default_args,
    description='Aposentadoria de CBIOS',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_aposentadoria_cbios", tooltip="ETL Aposentadoria CBIOS") as etl_aposentadoria_cbios:
        run_rw_aposentadoria_cbios = exec_job(
            task_id="extraction_aposentadoria_cbios",
            job_name="cr-juridico-extracao-aposentadoria-cbios-job-dev"
        )
        pop_td_aposentadoria_cbios = populate_table(
            table="td_ext_anp.aposentadoria_cbios",
            sql_name="/sql/trusted/dml_td_aposentadoria_cbios.sql"
        )
        run_rw_aposentadoria_cbios >> pop_td_aposentadoria_cbios

    etl_aposentadoria_cbios
