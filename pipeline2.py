from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='kubectl_job_dag',
    default_args=default_args,
    description='Run Kubernetes jobs using kubectl',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kubernetes', 'kubectl'],
)

# Create and run a Kubernetes Job
create_and_run_job = BashOperator(
    task_id='run_k8s_job',
    bash_command='''
    # Create a temporary job YAML
    cat << EOF > /tmp/airflow-job-{{ ts_nodash }}.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-task-{{ ts_nodash }}
  namespace: default
spec:
  template:
    spec:
      containers:
      - name: task-container
        image: python:3.9-slim
        command: ["python", "-c"]
        args: ["print('Hello from Kubernetes Job!'); import time; time.sleep(10)"]
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
      restartPolicy: Never
  backoffLimit: 3
EOF

    # Apply the job
    kubectl apply -f /tmp/airflow-job-{{ ts_nodash }}.yaml
    
    # Wait for job completion
    kubectl wait --for=condition=complete --timeout=300s job/airflow-task-{{ ts_nodash }}
    
    # Get job logs
    POD_NAME=$(kubectl get pods --selector=job-name=airflow-task-{{ ts_nodash }} -o jsonpath='{.items[0].metadata.name}')
    kubectl logs $POD_NAME
    
    # Cleanup
    kubectl delete job airflow-task-{{ ts_nodash }}
    rm /tmp/airflow-job-{{ ts_nodash }}.yaml
    ''',
    dag=dag,
)
