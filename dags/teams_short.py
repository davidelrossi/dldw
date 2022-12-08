# Libraries for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Libraries for FPL API
import requests


# Define the functions

# 1. Get data from URL

def get_teams_url():
    # API URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    response = requests.get(url)

    return response

# Create DAG
default_args = {
    'owner': 'Davide',
    'start_date': datetime.datetime(2022, 12, 8)
}

dag = DAG('teams_dag',
          schedule_interval='0 8 * * *',
          catchup=False,
          default_args=default_args)

#  Set Tasks
# 1. Get data from URL
get_teams_url = PythonOperator(
    task_id="get_teams_url",
    python_callable=get_teams_url,
    do_xcom_push=True,
    dag=dag
)
