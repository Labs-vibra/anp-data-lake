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

    #TaskGroup para o arquivo Histórico de Vendas
    with TaskGroup("etl_historico_vendas", tooltip="ETL Histórico de Vendas") as etl_historico_vendas:
        run_raw_historico_vendas = exec_cloud_run_job(
            task_id="raw_historico_vendas",
            job_name="cr-juridico-raw-vendas-atual-job-dev"
        )
        pop_td_historico_vendas = populate_table(
            table="td_ext_anp.liquidos_vendas_atual",
            sql_name="/sql/trusted/dml_td_liquidos_vendas_atual.sql"
        )
        run_raw_historico_vendas >> pop_td_historico_vendas

    #TaskGroup para o arquivo Histórico de Entregas (Fornecimento)
    with TaskGroup("etl_historico_entregas", tooltip="ETL Histórico de Entregas") as etl_historico_entregas:
        run_raw_historico_entregas = exec_cloud_run_job(
            task_id="raw_historico_entregas",
            job_name="cr-juridico-raw-liquidos-entrega-historico-job-dev"
        )

        pop_td_historico_entregas = populate_table(
            table="td_ext_anp.liquidos_entrega_historico",
            sql_name="/sql/trusted/dml_td_liquidos_historico_entregas.sql"
        )
        run_raw_historico_entregas >> pop_td_historico_entregas

    #TaskGroup para o arquivo Distribuidor Atual
    with TaskGroup("etl_distribuidor_atual", tooltip="ETL Distribuidor Atual") as etl_distribuidor_atual:
        run_raw_distribuidor_atual = exec_cloud_run_job(
            task_id="raw_distribuidor_atual",
            job_name="cr-juridico-raw-distribuidor-atual-job-dev"
        )

        pop_td_distribuidor_atual = populate_table(
            table="td_ext_anp.market_share_distribuidor_atual",
            sql_name="/sql/trusted/dml_td_market_share_distribuidor_atual.sql"
        )
        run_raw_distribuidor_atual >> pop_td_distribuidor_atual

    # TaskGroup para o arquivo Importação Distribuidores
    with TaskGroup("etl_importacao_distribuidores", tooltip="ETL Importação Distribuidores") as etl_importacao_distribuidores:
        run_raw_importacao_distribuidores = exec_cloud_run_job(
            task_id="raw_importacao_distribuidores",
            job_name="cr-juridico-raw-importacao-distribuidores-job-dev"
        )

        pop_td_importacao_distribuidores = populate_table(
            table="td_ext_anp.liquidos_importacao_distribuidores",
            sql_name="/sql/trusted/dml_td_liquidos_importacao_distribuidores.sql"
        )
        run_raw_importacao_distribuidores >> pop_td_importacao_distribuidores

    # TaskGroup para o arquivo Vendas Atual
    with TaskGroup("etl_vendas_atual", tooltip="ETL Vendas Atual") as etl_vendas_atual:
        run_raw_vendas_atual = exec_cloud_run_job(
            task_id="raw_vendas_atual",
            job_name="cr-juridico-raw-vendas-atual-job-dev"
        )

        pop_td_vendas_atual = populate_table(
            table="td_ext_anp.liquidos_vendas_atual",
            sql_name="/sql/trusted/dml_td_liquidos_vendas_atual.sql"
        )

        run_raw_vendas_atual >> pop_td_vendas_atual

    #TaskGroup para o arquivo Entregas Fornecedor Atual
    with TaskGroup("etl_entregas_fornecedor_atual", tooltip="ETL Entregas Fornecedor Atual") as etl_entregas_fornecedor_atual:
        run_raw_entregas_fornecedor_atual = exec_cloud_run_job(
            task_id="raw_entregas_fornecedor_atual",
            job_name="cr-juridico-raw-entregas-fornecedor-atual-job-dev"
        )

        pop_td_entregas_fornecedor_atual = populate_table(
            table="td_ext_anp.entregas_fornecedor_atual",
            sql_name="/sql/trusted/dml_td_entregas_fornecedor_atual.sql"
        )

        run_raw_entregas_fornecedor_atual >> pop_td_entregas_fornecedor_atual

    run_extraction_market_share >> [
        etl_historico_vendas,
        etl_historico_entregas,
        etl_distribuidor_atual,
        etl_importacao_distribuidores,
        etl_vendas_atual,
        etl_entregas_fornecedor_atual,
    ]
