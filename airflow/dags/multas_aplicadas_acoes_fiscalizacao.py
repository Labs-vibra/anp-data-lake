from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from utils.operators import exec_job, populate_table

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'multas_aplicadas_acoes_fiscalizacao',
    default_args=default_args,
    description='Pipeline ETL para multas aplicadas e ações de fiscalização da ANP',
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    tags=['anp', 'multas', 'fiscalizacao'],
) as dag:

    # Tarefa 1: Executar job Cloud Run para extração (download dos CSVs)
    extraction = exec_job(
        task_id='extract_multas_aplicadas_acoes_fiscalizacao',
        job_name='cr-juridico-multas-aplicadas-acoes-fiscalizacao-job-dev',
    )

    # Tarefa 2: Executar job Cloud Run para processar e carregar na camada raw
    extract_raw_data = exec_job(
        task_id='extract_raw_multas_aplicadas_acoes_fiscalizacao',
        job_name='cr-juridico-rw-multas-aplicadas-job-dev',
    )

    # Tarefa 3: Popular tabela trusted
    trusted_data = populate_table(
        task_id='populate_trusted_multas_aplicadas_acoes_fiscalizacao',
        table='td_ext_anp.multas_aplicadas_acoes_fiscalizacao',
        sql_name='dml_td_multas_aplicadas_acoes_fiscalizacao.sql'
    )

    extraction >> extract_raw_data >> trusted_data
