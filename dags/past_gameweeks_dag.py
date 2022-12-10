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


# 3. Get all past gameweeks info for all players

def get_gameweeks(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_players_ids'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    ids_list = data[0]

    # Empty dataframe
    gameweeks_df = pd.DataFrame()

    # Progress counter
    counter = 0

    # Loop through the urls
    for i in ids_list:
        url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
        r = requests.get(url_details).json()

        df = pd.json_normalize(r['history'])

        gameweeks_df = pd.concat([gameweeks_df, df])

        counter += 1
        if (counter % 50) == 0 or counter == len(ids_list):
            print(str(counter) + " players")

    # change data types
    gameweeks_df['influence'] = gameweeks_df['influence'].astype('float')
    gameweeks_df['creativity'] = gameweeks_df['creativity'].astype('float')
    gameweeks_df['threat'] = gameweeks_df['threat'].astype('float')
    gameweeks_df['ict_index'] = gameweeks_df['ict_index'].astype('float')
    gameweeks_df['expected_goals'] = gameweeks_df['expected_goals'].astype('float')
    gameweeks_df['expected_assists'] = gameweeks_df['expected_assists'].astype('float')
    gameweeks_df['expected_goal_involvements'] = gameweeks_df['expected_goal_involvements'].astype('float')
    gameweeks_df['expected_goals_conceded'] = gameweeks_df['expected_goals_conceded'].astype('float')

    gameweeks_list = [tuple(x) for x in gameweeks_df.to_numpy()]

    return gameweeks_list


# 4. Load fixtures data into database
def load_gameweeks(ti):

    # Get data returned from previous tasks'
    data = ti.xcom_pull(task_ids=['get_gameweeks'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Get data from nested list
    gameweeks_data = data[0]

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Drop existing table
    drop_table = "DROP TABLE IF EXISTS gameweeks;"

    # Create new table
    create_table = "CREATE TABLE IF NOT EXISTS gameweeks (element INT, fixture INT, opponent_team INT, total_points INT,\
    was_home VARCHAR(255), kickoff_time VARCHAR(255), team_h_score INT, team_a_score INT, round INT, minutes INT,\
    goals_scored INT, assists INT, clean_sheets INT, goals_conceded INT, own_goals INT, penalties_saved INT,\
    penalties_missed INT, yellow_cards INT, red_cards INT, saves INT, bonus INT, bps INT, influence FLOAT,\
    creativity FLOAT, threat FLOAT, ict_index FLOAT, starts INT, expected_goals FLOAT, expected_assists FLOAT,\
    expected_goal_involvements FLOAT, expected_goals_conceded FLOAT, value INT, transfers_balance INT, selected INT,\
    transfers_in INT, transfers_out INT);"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Commit
    pg_conn.commit()

    # Insert the rows into the database
    pg_hook.insert_rows(table="gameweeks", rows=gameweeks_data)


# 5. Log the end of the DAG
def finish_dag():
    logging.info('DAG HAS FINISHED,OBTAINED FIXTURES INFORMATION')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2022, 12, 9)
}

dag = DAG('fixtures_dag',
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
get_gameweeks = PythonOperator(
    task_id="get_gameweeks",
    python_callable=get_gameweeks,
    do_xcom_push=True,
    dag=dag
)

# 3. Load data
load_gameweeks = PythonOperator(
    task_id="load_gameweeks",
    python_callable=load_gameweeks,
    dag=dag
)

# 5. End Task
end_task = PythonOperator(
    task_id="end_task",
    python_callable=finish_dag,
    dag=dag
)

# -------------------- Triggering tasks -------------------- #

start_task >> get_players_ids >> get_gameweeks >> load_gameweeks >> end_task
