# created this test because, when added MinIO connection in Airflow, clicking in Test was not working.
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_minio_connection():
    hook = S3Hook(aws_conn_id='minio_conn')
    try:
        # get_conn() returns the boto3 client
        client = hook.get_conn()
        buckets = client.list_buckets()
        print(f"Connected! Found buckets: {buckets.get('Buckets', [])}")
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        raise

with DAG(
    dag_id='test_minio_dag',
    tags=["Training", "minio", "s3"],
    default_args={'owner': 'angie'},
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_step = PythonOperator(
        task_id='test_step',
        python_callable=test_minio_connection
    )