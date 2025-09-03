from asyncio import TaskGroup
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.operators import exec_cloud_run_job, populate_table

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
    run_extraction_market_share = exec_cloud_run_job(
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
    
    with TaskGroup("etl_historico_vendas", tooltip="ETL Histórico de Vendas") as etl_historico_vendas:

        run_raw_historico_vendas = exec_cloud_run_job(
            task_id="raw_historico_vendas",
            job_name="cr-juridico-raw-historico-vendas-job-dev"
        )

        pop_td_historico_vendas = populate_table(
            table="td_ext_anp.liquidos_entrega_historico",
            sql_name="/sql/trusted/dml_td_liquidos_vendas_historico.sql"
        )

        run_raw_historico_vendas >> pop_td_historico_vendas

    with TaskGroup("etl_historico_entregas", tooltip="ETL Histórico de Entregas") as etl_historico_entregas:
        run_raw_historico_entregas = exec_cloud_run_job(
            task_id="raw_historico_entregas",
            job_name="cr-juridico-raw-historico-entregas-job-dev"
        )

        pop_td_historico_entregas = populate_table(
            table="td_ext_anp.liquidos_entrega_historico",
            sql_name="/sql/trusted/dml_td_liquidos_entrega_historico.sql"
        )

        run_raw_historico_entregas >> pop_td_historico_entregas

    run_raw_historico_vendas = exec_cloud_run_job(
        task_id="raw_historico_vendas",
        job_name="cr-juricido-raw-historico-vendas-job-dev"
    )

    run_raw_vendas_atual = exec_cloud_run_job(
        task_id="raw_vendas_atual",
        job_name="cr-juridico-raw-vendas-atual-job-dev"
    )

    run_raw_entregas_fornecedor_atual = exec_cloud_run_job(
        task_id="raw_entregas_fornecedor_atual",
        job_name="cr-juridico-raw-entregas-fornecedor-atual-job-dev"
    )

    run_rw_market_share >> [run_raw_distribuidor_atual,
                            run_raw_importacao_distribuidores,
                            run_raw_historico_entregas,
                            run_raw_importacao_distribuidores,
                            run_raw_vendas_atual
                            run_raw_entregas_fornecedor_atual,
                            run_raw_historico_vendas,
                            etl_historico_vendas,
                            etl_historico_entregas
                           ]
