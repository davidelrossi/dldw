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
