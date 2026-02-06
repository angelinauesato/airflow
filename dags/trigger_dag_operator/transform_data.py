from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

def _cleaning():
    print('Clearning from fetch data DAG')

with DAG('transform_data', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False,
    tags=["Training", "TriggerDagRunOperator", "PythonOperator"]) as dag:

    save_raw = BashOperator(
        task_id='save_raw',
        bash_command='sleep 30'
    )

    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=_cleaning
    )

    save_raw >> cleaning_data
