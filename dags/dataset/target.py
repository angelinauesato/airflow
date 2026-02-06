from airflow.decorators import dag, task

from datetime import datetime
from include.dataset_trigger import file

@dag(
    schedule=[file],
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["Training", "include", "dataset"],
)
def target_dag():
    @task()
    def read_dataset():
        with open(file.uri, "r") as dataset_file:
            print(dataset_file.read())
    read_dataset()

target_dag()
