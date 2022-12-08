# Libraries for DAG
import datetime
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

# 2. Convert the data in json format

def convert_to_json(ti):

    # get data returned from 'scrape_team_urls_task'
    response = ti.xcom_pull(task_ids=['get_teams_url'])
    if not response:
        raise ValueError('No value currently stored in XComs')

    # Convert the data in json format
    data = response.json()

    return data


# Create DAG
default_args = {
    'owner': 'Davide',
    'start_date': datetime.datetime(2022,12,8)
}

dag = DAG('teams_dag',
          schedule_interval = '0 8 * * *',
          catchup = False,
          default_args = default_args)

#  Set Tasks
# 1. Get data from URL
get_teams_url = PythonOperator(
    task_id = "get_teams_url",
    python_callable = get_teams_url,
    do_xcom_push = True,
    dag = dag
)

# 2. Retrieve player urls from data lake
convert_to_json = PythonOperator(
    task_id = "convert_to_json",
    python_callable = convert_to_json,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

get_teams_url >> convert_to_json
