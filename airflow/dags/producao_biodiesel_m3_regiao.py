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
    dag_id='producao_biodiesel_m3_regiao_pipeline',
    default_args=default_args,
    description='Produção de biodiesel m3 regiao',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_producao_biodiesel_m3_regiao", tooltip="ETL Produção de biodiesel m3 regiao") as etl_producao_biodiesel_m3_regiao:
        run_rw_producao_biodiesel_m3_regiao = exec_job(
            task_id="producao_biodiesel_regiao_m3",
            job_name="cr-juridico-extracao-producao-biodiesel-m3-regiao-job-dev"
        )
        pop_td_producao_biodiesel_m3_regiao = populate_table(
            table="td_ext_anp.producao_biodiesel_m3_regiao",
            sql_name=f"/sql/trusted/dml_td_producao_biodiesel_m3_regiao.sql"
        )

        run_rw_producao_biodiesel_m3_regiao >> pop_td_producao_biodiesel_m3_regiao
