# -------------------- Libraries -------------------- #

# Libraries for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Libraries for FPL API
import requests
import pandas as pd

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# -------------------- Functions -------------------- #


# 1. Log the start of the DAG
def start_dag():
    logging.info('STARTING THE DAG, OBTAINING FIXTURES INFORMATION')


# 2. Get data from URL

def get_data():
    # API url
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    # Get data from url
    response = requests.get(url)

    # Convert the data in json format
    data = response.json()

    # Create list of all players' ids
    elements = pd.DataFrame(data['elements'])
    ids_list = elements['id'].tolist()

    return ids_list


# 3. Log the end of the DAG
def finish_dag():
    logging.info('DAG HAS FINISHED,OBTAINED FIXTURES INFORMATION')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 9)
}

dag = DAG('fixtures_dag',
          schedule_interval='0 8 * * *',
          catchup=False,
          default_args=default_args)

# -------------------- Set tasks -------------------- #

# 1. Start Task
start_task = PythonOperator(
    task_id="start_task",
    python_callable=start_dag,
    dag=dag
)

# 2. Get data from URL
get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    do_xcom_push=True,
    dag=dag
)

# 3. End Task
end_task = PythonOperator(
    task_id="end_task",
    python_callable=finish_dag,
    dag=dag
)

# -------------------- Triggering tasks -------------------- #

start_task >> get_data >> end_task
