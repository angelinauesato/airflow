from airflow  import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Added S3Hook
import pandas as pd
import requests
import json

def load_covid_data(ti):
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    data = response.json()
    
    # Push the raw data to XCom so the next task can upload it
    ti.xcom_push(key='raw_json_data', value=data)
    
    quantity = len(data)
    return quantity

def upload_to_minio(ti):
    # Pull data from the previous task
    data = ti.xcom_pull(task_ids='load_covid_data', key='raw_json_data')
    
    # Initialize S3Hook for MinIO
    s3 = S3Hook(aws_conn_id='minio_conn')
    
    bucket = "airflow"
    file_key = "airflow/tutorial_load_data/covid_data.json"

    # --- CHECK IF FILE EXISTS ---
    if s3.check_for_key(key=file_key, bucket_name=bucket):
        print(f"File {file_key} already exists in bucket {bucket}. Skipping upload.")
        return # Task ends here successfully

    # Upload to MinIO
    s3.load_string(
        string_data=json.dumps(data),
        key="airflow/tutorial_load_data/covid_data.json",
        bucket_name="airflow",
        replace=True
    )

def Is_valid_check(ti):
    quantity = ti.xcom_pull(task_ids='load_covid_data')
    if quantity > 1000:
        return 'valid_check'
    return 'nvalid_check'


with DAG('ny_covid_data', start_date = datetime(2021, 12, 1), schedule_interval = '30 * * * *', catchup = False,
        tags=["json","postgres","Training", "minio", "s3"],
        default_args={'owner': 'angie'}) as dag:

    load_covid_data = PythonOperator(
        task_id = 'load_covid_data',
        python_callable = load_covid_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio
    )

    Is_valid_check = BranchPythonOperator(
        task_id = 'Is_valid_check',
        python_callable = Is_valid_check
    )

    valid_check = BashOperator(
        task_id = 'valid_check',
        bash_command = "echo 'Quantity OK'"
    )

    nvalid_check = BashOperator(
        task_id = 'nvalid_check',
        bash_command = "echo 'Quantity is not OK'"
    )

    load_covid_data >> upload_task >> Is_valid_check >> [valid_check, nvalid_check] 