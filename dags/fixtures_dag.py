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

# -------------------- functions -------------------- #


# 1. Log the start of the DAG
def start_dag():
    logging.info('STARTING THE DAG, OBTAINING FIXTURES INFORMATION')


# 2. Get players IDs

def get_players_ids():
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


# 3. Get all fixtures info for all players

def get_fixtures(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_players_ids'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    ids_list = data[0]

    # Empty dataframe
    fixtures_df = pd.DataFrame()

    # Progress counter
    counter = 0

    # Loop through the urls
    for i in ids_list:
        url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
        r = requests.get(url_details).json()

        df = pd.json_normalize(r['fixtures'])

        fixtures_df = pd.concat([fixtures_df, df])

        counter += 1
        if (counter % 50) == 0 or counter == len(ids_list):
            print(str(counter) + " players")

    fixtures_list = [tuple(x) for x in fixtures_df.to_numpy()]

    return fixtures_list


# 4. Load fixtures data into database
def load_fixtures(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_fixtures'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    fixtures_data = data[0]

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Drop existing table
    drop_table = "DROP TABLE IF EXISTS fixtures;"

    # Create new table
    create_table = "CREATE TABLE IF NOT EXISTS fixtures (id INT, code INT, team_h INT, team_h_score VARCHAR(255),\
    team_a INT, team_a_score VARCHAR(255), event FLOAT, finished VARCHAR(255), minutes INT,\
    provisional_start_time VARCHAR(255), kickoff_time VARCHAR(255), event_name VARCHAR(255), is_home VARCHAR(255),\
    difficulty INT);"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Commit
    pg_conn.commit()

    # Insert the rows into the database
    pg_hook.insert_rows(table="fixtures", rows=fixtures_data)


# 5. Log the end of the DAG
def finish_dag():
    logging.info('DAG HAS FINISHED,OBTAINED FIXTURES INFORMATION')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 9)
}

dag = DAG('fixtures_dag',
          schedule_interval='0 12 * * *',
          catchup=False,
          default_args=default_args)

# -------------------- Set tasks -------------------- #

# 1. Start Task
start_task = PythonOperator(
    task_id="start_task",
    python_callable=start_dag,
    dag=dag
)

# 2. Get players IDs
get_players_ids = PythonOperator(
    task_id="get_players_ids",
    python_callable=get_players_ids,
    do_xcom_push=True,
    dag=dag
)

# 3. Get fixtures
get_fixtures = PythonOperator(
    task_id="get_fixtures",
    python_callable=get_fixtures,
    do_xcom_push=True,
    dag=dag
)

# 4. Load data
load_fixtures = PythonOperator(
    task_id="load_fixtures",
    python_callable=load_fixtures,
    dag=dag
)

# 5. End Task
end_task = PythonOperator(
    task_id="end_task",
    python_callable=finish_dag,
    dag=dag
)

# -------------------- Triggering tasks -------------------- #

start_task >> get_players_ids >> get_fixtures >> load_fixtures >> end_task
