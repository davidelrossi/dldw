# -------------------- Libraries -------------------- #

# Libraries for DAG
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Libraries for FPL API
import requests
import pandas as pd

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

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

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='fpl_api'
    )

    pg_hook.pandas_to_postgres(
        dataframe=teams_df,
        table_name="teams",
        if_exists="replace"
    )


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
    dag=dag
)
