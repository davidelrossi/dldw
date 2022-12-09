# -------------------- Libraries -------------------- #

# Libraries for DAG
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Libraries for FPL API
import requests
import pandas as pd

# -------------------- Functions -------------------- #


# 1. Get data from URL

def get_data():
    # API URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    response = requests.get(url)

    # Convert the data in json format
    data = response.json()

    # Create DataFrame
    teams_df = pd.DataFrame(data['teams'])

    return teams_df


def load_data(ti):
    # get data returned from previous task
    data = ti.xcom_pull(task_ids=['get_data'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    return data


# Create DAG
default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 8)
}

dag = DAG('tea_dag',
          schedule_interval='0 8 * * *',
          catchup=False,
          default_args=default_args)

# -------------------- Set tasks -------------------- #
# 1. Get data from URL
get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    do_xcom_push=True,
    dag=dag
)

# 2. Load data into database
load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag
)

# -------------------- Trigger tasks -------------------- #

get_data >> load_data
