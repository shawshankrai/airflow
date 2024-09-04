from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# Create dataset
my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file2.txt")

with DAG(
    dag_id="producer", schedule="@daily", start_date=datetime(2024, 8, 30), catchup=False
):

    # If two tasks update the same dataset, as soon as one is done, 
    # that triggers the Consumer DAG immediately without waiting for the second task to complete.
    
    # what task updates the dataset
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f: f.write("producer update")
        print("write complete")
    update_dataset()
    
    # what task updates the dataset
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f: f.write("producer update")
        print("write complete")
    update_dataset_2()
