from airflow.decorators import dag, task

from datetime import datetime
from include.dataset_trigger import file


@dag(
    schedule="@daily",
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["Training", "include", "dataset"],
)
def dataset_trigger():
    @task(outlets=[file])
    def update_dataset():
        with open(file.uri, "a+") as dataset:
            dataset.write("Update dataset and trigger another dag.")
    update_dataset()

dataset_trigger()
