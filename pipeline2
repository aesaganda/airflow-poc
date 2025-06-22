from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_kubernetes_pod_dag',
    default_args=default_args,
    description='A DAG that runs tasks in dedicated K8s pods',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kubernetes', 'example'],
) as dag:

    task = KubernetesPodOperator(
        task_id='print_hello_k8s',
        name='hello-task-pod',
        namespace='default',  # or your specific namespace
        image='python:3.9-slim',  # Use appropriate image
        cmds=['python', '-c'],
        arguments=['print("Hello from dedicated Kubernetes pod!")'],
        # Pod will be deleted after completion
        is_delete_operator_pod=True,
        # Get logs from the pod
        get_logs=True,
        # Optional: specify resources
        resources={
            'request_memory': '128Mi',
            'request_cpu': '100m',
            'limit_memory': '512Mi',
            'limit_cpu': '500m',
        },
        # Optional: specify node selector, tolerations, etc.
        # node_selector={'node-type': 'worker'},
        # tolerations=[...],
    )
