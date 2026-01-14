from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG('dag_execute_sql', start_date = datetime(2025,11,19),
         schedule_interval = '30 * * * *', catchup=False,
         tags=["postgres", "templates", "Training"],
         default_args={'owner': 'angie'},
         template_searchpath='/opt/airflow/sql/',
        ) as dag:

    create_table_db = PostgresOperator(
        task_id = 'create_table_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'create_tb_client.sql'
    )

    insert_data_tb_client = PostgresOperator(
        task_id = 'insert_data_tb_client',
        postgres_conn_id = 'postgres-airflow',
        sql = 'insert_data_tb_client.sql'
    )

    create_table_db >> insert_data_tb_client