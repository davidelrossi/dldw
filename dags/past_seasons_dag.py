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


# 3. Get all past seasons info for all players

def get_seasons(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_players_ids'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    ids_list = data[0]

    # Empty dataframe
    past_seasons_df = pd.DataFrame()

    # Progress counter
    counter = 0

    for i in ids_list:
        url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
        r = requests.get(url_details).json()

        df = pd.json_normalize(r['history_past'])

        past_seasons_df = pd.concat([past_seasons_df, df])

        counter += 1
        if (counter % 50) == 0 or counter == len(ids_list):
            print(str(counter) + " players")

    # Change data types
    past_seasons_df['influence'] = past_seasons_df['influence'].astype('float')
    past_seasons_df['creativity'] = past_seasons_df['creativity'].astype('float')
    past_seasons_df['threat'] = past_seasons_df['threat'].astype('float')
    past_seasons_df['ict_index'] = past_seasons_df['ict_index'].astype('float')
    past_seasons_df['expected_goals'] = past_seasons_df['expected_goals'].astype('float')
    past_seasons_df['expected_assists'] = past_seasons_df['expected_assists'].astype('float')
    past_seasons_df['expected_goal_involvements'] = past_seasons_df['expected_goal_involvements'].astype('float')
    past_seasons_df['expected_goals_conceded'] = past_seasons_df['expected_goals_conceded'].astype('float')

    past_seasons_list = [tuple(x) for x in past_seasons_df.to_numpy()]

    return past_seasons_list


# 4. Load past seasons data into database
def load_seasons(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_seasons'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    seasons_data = data[0]

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Drop existing table
    drop_table = "DROP TABLE IF EXISTS past_seasons;"

    # Create new table
    create_table = "CREATE TABLE IF NOT EXISTS past_seasons (season_name VARCHAR(255), element_code INT, start_cost INT,\
            end_cost INT, total_points INT, minutes INT, goals_scored INT, assists INT, clean_sheets INT,\
            goals_conceded INT, own_goals INT, penalties_saved INT, penalties_missed INT, yellow_cards INT,\
            red_cards INT, saves INT, bonus INT, bps INT, influence FLOAT, creativity FLOAT, threat FLOAT,\
            ict_index FLOAT, starts INT, expected_goals FLOAT,expected_assists FLOAT, expected_goal_involvements FLOAT,\
            expected_goals_conceded FLOAT);"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Commit
    pg_conn.commit()

    # Insert the rows into the database
    pg_hook.insert_rows(table="past_seasons", rows=seasons_data)


# 5. Log the end of the DAG
def finish_dag():
    logging.info('DAG HAS FINISHED,OBTAINED FIXTURES INFORMATION')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 9)
}

dag = DAG('seasons_dag',
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

# 2. Get players IDs
get_players_ids = PythonOperator(
    task_id="get_players_ids",
    python_callable=get_players_ids,
    do_xcom_push=True,
    dag=dag
)

# 3. Get fixtures
get_seasons = PythonOperator(
    task_id="get_seasons",
    python_callable=get_seasons,
    do_xcom_push=True,
    dag=dag
)

# 3. Load data
load_seasons = PythonOperator(
    task_id="load_seasons",
    python_callable=load_seasons,
    dag=dag
)

# 5. End Task
end_task = PythonOperator(
    task_id="end_task",
    python_callable=finish_dag,
    dag=dag
)

# -------------------- Triggering tasks -------------------- #

start_task >> get_players_ids >> get_seasons >> load_seasons >> end_task
