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

# 3. Load data into database

def load_data(ti):
    # get data returned from 'scrape_team_urls_task'
    data = ti.xcom_pull(task_ids=['convert_to_json'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Create DataFrame
    teams_df = pd.DataFrame(data['teams'])

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='fpl_api'
    )

    # SQL statements


    drop_table = "DROP TABLE IF EXISTS Teams;"

    create_table ="CREATE TABLE IF NOT EXISTS Teams (code INT, draw INT, form VARCHAR(255),\
            id INT, loss INT, name VARCHAR(255),\
            played INT, points INT, position INT, short_name VARCHAR(255), strength INT, team_division VARCHAR(255),\
            unavailable VARCHAR(255), win INT, strength_overall_home INT, strength_overall_away INT,\
            strength_attack_home INT, strength_attack_away INT, strength_defence_home INT, strength_defence_away INT,\
            pulse_id INT);"

    # Generate the column names for the INSERT statement
    cols = ', '.join(teams_df.columns)

    # Generate placeholders for the values
    placeholders = ', '.join(['%s'] * len(teams_df.columns))

    insert = "INSERT INTO Teams ({}) VALUES ({})".format(cols, placeholders)

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Insert data into Data Lake
    cursor.executemany(insert, teams_df)
    pg_conn.commit()
    print("The Dataframe is Inserted")


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
    do_xcom_push = True,
    dag = dag
)

# 3. Scraping
load_data = PythonOperator(
    task_id = "load_data",
    python_callable = load_data,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> convert_to_json >> load_data