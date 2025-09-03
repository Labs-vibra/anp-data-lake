from airflow import DAG
from airflow.utils.dates import days_ago
from utils.operators import exec_cloud_run_job

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='market_share_pipeline',
    default_args=default_args,
    description='Market Share pipeline',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:
    run_extracao_market_share = exec_cloud_run_job(
        task_id="extraction_market_share",
        job_name="cr-juridico-extracao-market-share-job-dev"
    )

    run_raw_distribuidor_atual = exec_cloud_run_job(
        task_id="raw_distribuidor_atual",
        job_name="cr-juridico-raw-distribuidor-atual-job-dev"
    )

    run_raw_importacao_distribuidores = exec_cloud_run_job(
        task_id="raw_importacao_distribuidores",
        job_name="cr-juridico-raw-importacao-distribuidores-job-dev"
    )

    run_raw_historico_entregas = exec_cloud_run_job(
        task_id="raw_historico_entregas",
        job_name="cr-juridico-raw-historico-entregas-job-dev"
    )

    
    run_raw_vendas_atual = exec_cloud_run_job(
        task_id="raw_vendas_atual",
        job_name="cr-juridico-raw-vendas-atual-job-dev"
    )
    
    run_rw_market_share >> [run_raw_distribuidor_atual,
                            run_raw_importacao_distribuidores,
                            run_raw_historico_entregas,
                            run_raw_importacao_distribuidores,
                            run_raw_vendas_atual
                           ]