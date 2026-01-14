# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/s3/s3.html
from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator


with DAG('dag_s3', start_date = datetime(2026, 1, 13), schedule_interval = '30 * * * *', catchup = False,
        tags=["Training", "minio", "s3", "sensor"],
        default_args={'owner': 'angie'}) as dag:
    
    sensor_file_s3 = S3KeySensor(
        task_id= "sensor_file_s3",
        aws_conn_id = "minio_conn",
        bucket_name="airflow",
        bucket_key="airflow/tutorial_load_data/covid_data.json",
        wildcard_match=True

    )

    is_ok = BashOperator(
        task_id="is_ok",
        bash_command="echo 'File just arrived!'"
    )

    sensor_file_s3 >> is_ok