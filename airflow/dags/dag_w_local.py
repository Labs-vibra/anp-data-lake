from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='local_docker_pipeline',
    default_args=default_args,
    description='Pipeline executando imagem Docker local',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
) as dag:

    # Task que executa uma imagem Docker local
    run_local_docker = DockerOperator(
        task_id='run_local_docker_task',
        image='extracao:latest',
        environment={
            'BUCKET_NAME': 'ext-ecole-biomassa',
            'GOOGLE_CLOUD_PROJECT': 'ext-ecole-biomassa-468317',
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/gcp.secrets.json'
        }
    )

    run_local_docker