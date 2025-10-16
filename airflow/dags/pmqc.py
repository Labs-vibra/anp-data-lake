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
    dag_id='pmqc_pipeline',
    default_args=default_args,
    description='PQMC - Processamento por ano para evitar timeout',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    anos = list(range(2016, 2026))

    tarefas_por_ano = []

    for ano in anos:
        with TaskGroup(f"etl_pmqc_{ano}", tooltip=f"ETL PMQC {ano}") as etl_pmqc_ano:

            run_rw_pmqc = exec_cloud_run_job(
                task_id=f"pmqc_raw_{ano}",
                job_name="cr-juridico-rw-pmqc-job-dev",
                args={
                    "env": [
                        {"name": "START_YEAR", "value": str(ano)},
                        {"name": "END_YEAR", "value": str(ano)},
                        {"name": "BUCKET_NAME", "value": "biel-labs-vibra-data-bucket"},
                        {"name": "GOOGLE_CLOUD_PROJECT", "value": "fundamentals-iac"},
                    ]
                }
            )

            tarefas_por_ano.append(run_rw_pmqc)

    pop_td_pmqc = populate_table(
        table="td_ext_anp.pmqc",
        sql_name="/sql/trusted/dml_td_pmqc.sql"
    )

    tarefas_por_ano >> pop_td_pmqc
