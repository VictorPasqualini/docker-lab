
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta

from config.constants import MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from tasks.task_load_api import api_to_minio_ingest_landing


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
    description='DAG inicial de ingestão dos dados da API de Pokémon para o MinIO',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def main_dag():
    table_names = ['pokemon']
    
    bucket_landing = 'landing'

    ingest_tasks = {}
    bronze_tasks = {}
    silver_tasks = {}
    
    with TaskGroup('task_group_ingest_pokeapi', tooltip='Tasks relacionadas à API de Pokémon') as task_group_ingest_pokeapi:
        
        for table_name in table_names:
            ingest_tasks[table_name] = PythonOperator(
                task_id=f'ingest_{table_name}',
                python_callable=api_to_minio_ingest_landing,
                op_kwargs={
                    'table_name': table_name,
                    'bucket_name': bucket_landing,
                    'endpoint_url': MINIO_ENDPOINT_URL,
                    'access_key': MINIO_ACCESS_KEY,
                    'secret_key': MINIO_SECRET_KEY,
                },
            )

    with TaskGroup('task_group_transform_bronze', tooltip='Tasks relacionadas ao processamento dos dados com Spark') as task_group_transform_bronze:
        
        for table_name in table_names:
            bronze_tasks[table_name] = SparkSubmitOperator(
                task_id=f'transform_bronze_{table_name}',
                application='/airflow/dags/jobs/job_transform_bronze.py',
                conn_id='spark_default',
                name=f'transform_bronze_{table_name}',
                conf={
                    "spark.master": "spark://spark-master:7077",
                    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT_URL,
                    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
                    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                },
                application_args=[
                    '--table_name', table_name,
                ],
            )


    for table_name in table_names:
        ingest_tasks[table_name] >> bronze_tasks[table_name]

main_dag_instance = main_dag()