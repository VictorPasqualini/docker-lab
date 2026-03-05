
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta

from config.constants import MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from tasks.task_load_api import api_to_minio_etl_landing


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_main',
    default_args=default_args,
    description='DAG principal para orquestrar o pipeline de dados através de tasks e grupos de tarefas',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def main_dag():
    table_names = ['pokemon']
    bucket_name = 'landing'
    

    with TaskGroup('task_group_pokeapi', tooltip='Tasks relacionadas à API de Pokémon') as task_group_pokeapi:
        
        for table_name in table_names:
            PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=api_to_minio_etl_landing,
                op_kwargs={
                    'table_name': table_name,
                    'bucket_name': bucket_name,
                    'endpoint_url': MINIO_ENDPOINT_URL,
                    'access_key': MINIO_ACCESS_KEY,
                    'secret_key': MINIO_SECRET_KEY,
                },
            )

    task_group_pokeapi

main_dag_instance = main_dag()