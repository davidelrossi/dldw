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
    logging.info('STARTING THE DAG, OBTAINING ELEMENT TYPES INFORMATION')


# 2. Get data from URL

def get_data():
    # API URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    response = requests.get(url)

    # Convert the data in json format
    data = response.json()

    # Create DataFrame
    element_types_df = pd.DataFrame(data['element_types'])

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Drop existing table
    drop_table = "DROP TABLE IF EXISTS element_types;"

    # Create new table
    create_table = "CREATE TABLE IF NOT EXISTS element_types (id INT, plural_name VARCHAR(255), plural_name_short VARCHAR(255),\
            singular_name VARCHAR(255), singular_name_short VARCHAR(255), squad_select INT, squad_min_play INT,\
            squad_max_play INT, ui_shirt_specific VARCHAR(255), sub_positions_locked VARCHAR(255), element_count INT);"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Commit
    pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in element_types_df.values]

    # Insert the rows into the database
    pg_hook.insert_rows(table="element_types", rows=rows)


# 3. Log the end of the DAG
def finish_dag():
    logging.info('DAG HAS FINISHED,OBTAINED ELEMENT TYPES INFORMATION')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 9)
}

dag = DAG('element_type_dag',
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
