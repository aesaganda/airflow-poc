from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def sample_task():
    print("Hello from the KubernetesExecutor task!")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='JOB12345',
    default_args=default_args,
    description='A simple DAG for KubernetesExecutor demo',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task = PythonOperator(
        task_id='print_hello',
        python_callable=sample_task,
    )

    task
