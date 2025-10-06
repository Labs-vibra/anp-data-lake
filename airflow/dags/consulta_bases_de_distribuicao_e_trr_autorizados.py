from airflow import DAG
from utils.operators import exec_cloud_run_job, populate_table
from airflow.utils.dates import days_ago # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='consulta_bases_de_distribuicao_e_trr_autorizados_pipeline',
    default_args=default_args,
    description='Consulta das Bases de Distribuição e de TRR Autorizados',
    schedule_interval='@monthly',
    catchup=False,
    max_active_tasks=2,
) as dag:

    with TaskGroup("etl_consulta_bases_de_distribuicao_e_trr_autorizados",
                   tooltip="ETL Consulta das Bases de Distribuição e TRR Autorizados") as etl_consulta_bases_de_distribuicao_e_trr_autorizado:
        run_extracao_consulta_bases_de_distribuicao_e_trr_autorizados = exec_cloud_run_job(
            task_id="consulta_bases_de_distribuicao_e_trr_autorizados_extracao",
            job_name="cr-juridico-extracao-bases-distribuicao-trr-autorizados-job"
        )
        run_rw_consulta_bases_de_distribuicao_e_trr_autorizados = exec_cloud_run_job(
            task_id="consulta_bases_de_distribuicao_e_trr_autorizados_raw",
            job_name="cr-juridico-rw-trr-autorizados-job-dev"
        )
        pop_td_consulta_bases_de_distribuicao_e_trr_autorizados = populate_table(
            table="td_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados",
            sql_name="/sql/trusted/dml_td_consulta_bases_de_distribuicao_e_trr_autorizados.sql"
        )
        run_extracao_consulta_bases_de_distribuicao_e_trr_autorizados >> run_rw_consulta_bases_de_distribuicao_e_trr_autorizados >> pop_td_consulta_bases_de_distribuicao_e_trr_autorizados

    etl_consulta_bases_de_distribuicao_e_trr_autorizado
