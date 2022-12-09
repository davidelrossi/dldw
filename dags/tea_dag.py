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
        postgres_conn_id='postgres_db'
    )

    drop_table = "DROP TABLE IF EXISTS Teams;"

    create_table = "CREATE TABLE IF NOT EXISTS Teams (code INT, draw INT, form VARCHAR(255),\
            id INT, loss INT, name VARCHAR(255),\
            played INT, points INT, position INT, short_name VARCHAR(255), strength INT, team_division VARCHAR(255),\
            unavailable VARCHAR(255), win INT, strength_overall_home INT, strength_overall_away INT,\
            strength_attack_home INT, strength_attack_away INT, strength_defence_home INT, strength_defence_away INT,\
            pulse_id INT);"


    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in teams_df.values]

    # Insert the rows into the database
    pg_hook.insert_rows(table="Teams", rows=rows)

    pg_conn.commit()


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
