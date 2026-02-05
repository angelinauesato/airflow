# TriggerDagRunOperator is reliable in terms that transform_data dag will wait for the fetch_data dag to finish
# then the second dag can start.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

def fetch_data_source():
    print('downloading')

with DAG('fetch_data', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    fetch_data_source = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_source
    )

    trigger_transformation = TriggerDagRunOperator(
        task_id = 'trigger_transformation',
        trigger_dag_id = 'transform_data',
        execution_date = '{{ ds }}', # make sure the execution date for both will be the same. It will fail if retry. 
        reset_dag_run=True, # Allows it to trigger again on re-runs 
        wait_for_completion=True, # It will wait the transform_data dag to finish, before marking completed
        poke_interval=30
    )
