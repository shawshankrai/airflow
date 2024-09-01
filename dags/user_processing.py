from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from pandas import json_normalize
from datetime import datetime

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )

# DAG
with DAG("user_processing", start_date=datetime(2024, 8, 30), schedule_interval="@daily", catchup=False) as dag:
    
    # Operator - PostgresOperator
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
                CREATE TABLE IF NOT EXISTS users(
                    firstname TEXT NOT NULL,
                    lastname TEXT NOT NULL,
                    country TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT NOT NULL
                );
            '''
    )

    # docker exec -it airflow-airflow-scheduler-1 bash
    # airflow tasks test user_processing create_table 2024-08-30

    # Sensor - Operator
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    # Connection details
    # Name: user_api
    # Connection type: HTTP
    # Host: https://randomuser.me/

    # Operator - SimpleHttpOperator
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # Operatore - python
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    # Operatore - python
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    # Dependency
    is_api_available >> extract_user >> process_user >> store_user

    # docker exec -it airflow-airflow-worker-1 bash
    # docker exec -it airflow-postgres-1 bash
        # psql -Uairflow
        # SELECT * FROM users;