from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file2.txt")

with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file_2], # wil trigger as soon as my file updates
    start_date=datetime(2024, 8, 30),
    catchup=False
):
    # Airflow monitors datasets only within the context of DAGs and Tasks.
    # If an external tool updates the actual data represented by a Dataset, 
    # Airflow has no way of knowing that.
    
    @task
    def read_dataset():
        with open(my_file.uri, "r") as f: print(f.read())
        print("read completed")      
    read_dataset()