from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='k8s_pod_operator_dag',
    default_args=default_args,
    description='Run task in a separate Kubernetes pod',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kubernetes', 'pod'],
)

run_in_pod = KubernetesPodOperator(
    task_id='run_python_task',
    name='run-python-task',
    namespace='default',
    image='python:3.9-slim',
    cmds=["python", "-c"],
    arguments=["print('Hello from KubernetesPodOperator!'); import time; time.sleep(10)"],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
