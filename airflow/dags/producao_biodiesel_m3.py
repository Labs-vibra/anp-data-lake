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
    dag_id='producao_biodiesel_m3_pipeline',
    default_args=default_args,
    description='Produção de biodiesel m3',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
) as dag:

    run_extraction_producao_biodiesel_m3 = exec_job(
        task_id="extracao_producao_biodiesel_m3",
        job_name="cr-juridico-extracao-producao-biodiesel-m3-job-dev"
    )

    with TaskGroup("etl_producao_biodiesel_m3", tooltip="ETL Produção de biodiesel m3") as etl_producao_biodiesel_m3:
        run_rw_producao_biodiesel_m3 = exec_job(
            task_id="producao_biodiesel_m3",
            job_name="cr-juridico-extracao-producao-biodiesel-m3-job-dev"
        )
        pop_td_producao_biodiesel_m3 = populate_table(
            table="td_ext_anp.producao_biodiesel_m3",
            sql_name="/sql/trusted/dml_td_producao_biodiesel_m3.sql"
        )
        run_rw_producao_biodiesel_m3 >> pop_td_producao_biodiesel_m3