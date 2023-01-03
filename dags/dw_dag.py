# -------------------- Libraries -------------------- #

# Libraries for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Libraries for FPL API
import requests
import pandas as pd
import numpy as np

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# -------------------- functions -------------------- #


# 1. Log the start of the DAG

def start_dag():

    logging.info('STARTING THE DAG, OBTAINING FIXTURES INFORMATION')


# 2. Creating gameweek staging table

def gw_stg_table():

    # Data Lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Connect to Data Lake
    conn_1 = pg_hook_1.get_conn()
    cur_1 = conn_1.cursor()

    # SQL Statement: Get data from gameweeks table
    sql_get_gw = "SELECT * FROM gameweeks;"

    cur_1.execute(sql_get_gw)
    tuples_gw = cur_1.fetchall()

    # Data Warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Connect to Data Warehouse
    conn_2 = pg_hook_2.get_conn()
    cur_2 = conn_2.cursor()

    # SQL Statement: Drop staging table
    sql_drop = "DROP TABLE IF EXISTS stage_gameweeks"

    # SQL Statement: Create staging table
    sql_create_gw_table = "CREATE TABLE IF NOT EXISTS stage_gameweeks (element INT, fixture INT, opponent_team INT,\
        total_points INT, was_home VARCHAR(255), kickoff_time VARCHAR(255), team_h_score INT, team_a_score INT,\
        round INT, minutes INT, goals_scored INT, assists INT, clean_sheets INT, goals_conceded INT, own_goals INT,\
        penalties_saved INT, penalties_missed INT, yellow_cards INT, red_cards INT, saves INT, bonus INT, bps INT,\
        influence FLOAT, creativity FLOAT, threat FLOAT, ict_index FLOAT, starts INT, expected_goals FLOAT,\
        expected_assists FLOAT, expected_goal_involvements FLOAT, expected_goals_conceded FLOAT, value INT,\
        transfers_balance INT, selected INT, transfers_in INT, transfers_out INT);"

    # Create and insert data into data warehouse staging table
    cur_2.execute(sql_drop)
    cur_2.execute(sql_create_gw_table)
    for row in tuples_gw:
        cur_2.execute('INSERT INTO stage_gameweeks VALUES %s', (row,))
    conn_2.commit()


# 3. Creating elements staging table

def el_stg_table():

    # Data Lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Connect to Data Lake
    conn_1 = pg_hook_1.get_conn()
    cur_1 = conn_1.cursor()

    # SQL Statement: Get data from elements table
    sql_get_el = "SELECT id, first_name, second_name, web_name, team, code FROM elements;"

    cur_1.execute(sql_get_el)
    tuples_el = cur_1.fetchall()

    # Data Warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Connect to Data Warehouse
    conn_2 = pg_hook_2.get_conn()
    cur_2 = conn_2.cursor()

    # SQL Statement: Drop staging table
    sql_drop = "DROP TABLE IF EXISTS stage_elements"

    # SQL Statement: Create staging table
    sql_create_el_table = "CREATE TABLE IF NOT EXISTS stage_elements (id VARCHAR(255), first_name VARCHAR(255),\
        second_name VARCHAR(255), web_name VARCHAR(255), team INT, code INT);"

    # Create and insert data into data warehouse staging table
    cur_2.execute(sql_drop)
    cur_2.execute(sql_create_el_table)
    for row in tuples_el:
        cur_2.execute('INSERT INTO stage_elements VALUES %s', (row,))
    conn_2.commit()


# 4. Creating elements_type staging table

def et_stg_table():

    # Data Lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Connect to Data Lake
    conn_1 = pg_hook_1.get_conn()
    cur_1 = conn_1.cursor()

    # SQL Statement: Get data from element_types table
    sql_get_et = "SELECT id, singular_name FROM element_types;"

    cur_1.execute(sql_get_et)
    tuples_et = cur_1.fetchall()

    # Data Warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Connect to Data Warehouse
    conn_2 = pg_hook_2.get_conn()
    cur_2 = conn_2.cursor()

    # SQL Statement: Drop staging table
    sql_drop = "DROP TABLE IF EXISTS stage_element_types"

    # SQL Statement: Create staging table
    sql_create_et_table = "CREATE TABLE IF NOT EXISTS stage_elements (id INT, singular_name VARCHAR(255));"

    # Create and insert data into data warehouse staging table
    cur_2.execute(sql_drop)
    cur_2.execute(sql_create_et_table)
    for row in tuples_et:
        cur_2.execute('INSERT INTO stage_elements VALUES %s', (row,))
    conn_2.commit()


# 5. Creating team staging table

def tm_stg_table():

    # Data Lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='postgres_db'
    )

    # Connect to Data Lake
    conn_1 = pg_hook_1.get_conn()
    cur_1 = conn_1.cursor()

    # SQL Statement: Get data from teams table
    sql_get_tm = "SELECT id, name FROM teams;"

    cur_1.execute(sql_get_tm)
    tuples_tm = cur_1.fetchall()

    # Data Warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Connect to Data Warehouse
    conn_2 = pg_hook_2.get_conn()
    cur_2 = conn_2.cursor()

    # SQL Statement: Drop staging table
    sql_drop = "DROP TABLE IF EXISTS stage_teams"

    # SQL Statement: Create staging table
    sql_create_tm_table = "CREATE TABLE IF NOT EXISTS stage_teams (id INT, name VARCHAR(255));"

    # Create and insert data into data warehouse staging table
    cur_2.execute(sql_drop)
    cur_2.execute(sql_create_tm_table)
    for row in tuples_tm:
        cur_2.execute('INSERT INTO stage_teams VALUES %s', (row,))
    conn_2.commit()


# 6. Transforming the data
def transform_data():

    # Data Warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='dw_stage'
    )

    # Connect to Data Warehouse
    conn_2 = pg_hook_2.get_conn()
    cur_2 = conn_2.cursor()

    # Get gameweeks data
    sql_select_gw = "SELECT * FROM stage_gameweeks;"
    cur_2.execute(sql_select_gw)

    # Get the column names from the cursor's description
    column_names = [desc[0] for desc in cur_2.description]

    # Fetch the results of the query and store them in a dataframe
    df_gw = pd.DataFrame(cur_2.fetchall(), columns=column_names)

    # Get elements data
    sql_select_el = "SELECT *  FROM stage_elements;"
    cur_2.execute(sql_select_el)

    # Get the column names from the cursor's description
    column_names = [desc[0] for desc in cur_2.description]

    # Fetch the results of the query and store them in a dataframe
    df_el = pd.DataFrame(cur_2.fetchall(), columns=column_names)

    # Get element_types data
    sql_select_et = "SELECT * FROM stage_element_types;"
    cur_2.execute(sql_select_et)

    # Get the column names from the cursor's description
    column_names = [desc[0] for desc in cur_2.description]

    # Fetch the results of the query and store them in a dataframe
    df_et = pd.DataFrame(cur_2.fetchall(), columns=column_names)

    # Get teams data
    sql_select_tm = "SELECT * FROM stage_teams;"
    cur_2.execute(sql_select_tm)

    # Get the column names from the cursor's description
    column_names = [desc[0] for desc in cur_2.description]

    # Fetch the results of the query and store them in a dataframe
    df_tm = pd.DataFrame(cur_2.fetchall(), columns=column_names)

    # Transform data

    # Set dict for position mapping
    dict_et = df_et.set_index('id')['singular_name'].to_dict()

    # Change position ID with position name
    df_el["element_type"] = df_el["element_type"].map(dict_et)

    # Rename column
    df_el.rename(columns={'element_type': 'position'}, inplace=True)

    # Change data type for id in elements dataframe
    df_el['id'] = df_el['id'].astype(int)

    # Merge gameweek dataframe with elements dataframe on "id"
    df_gw = df_gw.merge(df_el, left_on='element', right_on='id', how='left')

    # Set dict for team mapping
    dict_tm = df_tm.set_index('id')['name'].to_dict()

    # Change team ID with team name
    df_gw["team"] = df_gw["team"].map(dict)
    df_gw["opponent_team"] = df_gw["opponent_team"].map(dict_tm)

    # Drop element column (duplicate)
    df_gw = df_gw.drop('element', axis=1)

    # Rearrange columns order for better visualisation
    first_cols = ['id', 'first_name', 'second_name', 'web_name', 'code', 'position', 'fixture', 'team', 'opponent_team']
    last_cols = [col for col in df_gw.columns if col not in first_cols]
    df_gw = df_gw[first_cols + last_cols]

    # Get fixtures with extended name
    df_gw['fixture'] = np.where(df_gw['was_home'] == 'true', df_gw['team'] + '-' + df_gw['opponent_team'],
                                df_gw['opponent_team'] + '-' + df_gw['team'])

    # Change kickoff time datetime type
    df_gw['kickoff_time'] = pd.to_datetime(df_gw['kickoff_time'], format='%Y-%m-%d')

    # Adding a column with just the date
    df_gw['date'] = pd.to_datetime(df_gw['kickoff_time']).dt.date

    # Change data type of "date" column to date
    df_gw['date'] = pd.to_datetime(df_gw['date'], format='%Y-%m-%d')

    # LOAD DATA
    # Data Warehouse credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Drop existing table
    drop_table = "DROP TABLE IF EXISTS store_gameweeks;"

    # Create new table
    create_table = "CREATE TABLE IF NOT EXISTS store_gameweeks (id INT, first_name VARCHAR(255),\
    second_name VARCHAR(255),web_name VARCHAR(255), code INT, position VARCHAR(255), fixture VARCHAR(255),\
    team VARCHAR(255), opponent_team VARCHAR(255), total_points INT, was_home VARCHAR(255), kickoff_time VARCHAR(255),\
    team_h_score INT, team_a_score INT, round INT, minutes INT, goals_scored INT, assists INT, clean_sheets INT,\
    goals_conceded INT, own_goals INT, penalties_saved INT, penalties_missed INT, yellow_cards INT, red_cards INT,\
    saves INT, bonus INT, bps INT, influence FLOAT, creativity FLOAT, threat FLOAT, ict_index FLOAT, starts INT,\
    expected_goals FLOAT, expected_assists FLOAT, expected_goal_involvements FLOAT, expected_goals_conceded FLOAT,\
    value INT, transfers_balance INT, selected INT, transfers_in INT, transfers_out INT, date Date);"

    # Connect to data warehouse
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(drop_table)
    cursor.execute(create_table)

    # Commit
    pg_conn.commit()

    # Create list with all data
    gameweeks_list = [tuple(x) for x in df_gw.to_numpy()]

    # Insert the rows into the database
    pg_hook.insert_rows(table="store_gameweeks", rows=gameweeks_list)


# 7. Drop staging tables

def drop_stg_tables():

    # Data Warehouse credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datawarehouse'
    )

    # Connect to Data Warehouse
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # SQL Statements: Drop staging tables
    sql_drop_gw = "DROP TABLE IF EXISTS stage_gameweeks"
    sql_drop_et = "DROP TABLE IF EXISTS stage_element_types"
    sql_drop_el = "DROP TABLE IF EXISTS stage_elements"
    sql_drop_tm = "DROP TABLE IF EXISTS stage_teams"

    # Execute sql statements
    cursor.execute(sql_drop_gw)
    cursor.execute(sql_drop_et)
    cursor.execute(sql_drop_el)
    cursor.execute(sql_drop_tm)
    conn.commit()


# 8. Log the end of the DAG

def finish_dag():
    logging.info('DAG HAS FINISHED, NEW GAMEWEEKS TABLE STORED')


# -------------------- Create DAG -------------------- #

default_args = {
    'owner': 'davide',
    'start_date': datetime.datetime(2023, 1, 4)
}

dag = DAG('datawarehouse_dag',
          schedule_interval='0 13 * * *',
          catchup=False,
          default_args=default_args)

# -------------------- Set tasks -------------------- #

# 1. Start Task
start_task = PythonOperator(
    task_id="start_task",
    python_callable=start_dag,
    dag=dag
)

# 2. Create GW staging table
gw_stg_table = PythonOperator(
    task_id="gw_stg_table",
    python_callable=gw_stg_table,
    dag=dag
)

# 3. Create EL staging table
el_stg_table = PythonOperator(
    task_id="el_stg_table",
    python_callable=el_stg_table,
    dag=dag
)

# 4. Create ET staging table
et_stg_table = PythonOperator(
    task_id="et_stg_table",
    python_callable=et_stg_table,
    dag=dag
)

# 5. Create TM staging table
tm_stg_table = PythonOperator(
    task_id="tm_stg_table",
    python_callable=tm_stg_table,
    dag=dag
)

# 6. Transforming and loading final data
transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)

# 7. Dropping staging tables
drop_stg_tables = PythonOperator(
    task_id="drop_stg_tables",
    python_callable=drop_stg_tables,
    dag=dag
)

# 5. End Task
end_task = PythonOperator(
    task_id="end_task",
    python_callable=finish_dag,
    dag=dag
)

# -------------------- Triggering tasks -------------------- #

start_task >> gw_stg_table >> el_stg_table >> et_stg_table >> tm_stg_table >> transform_data >> drop_stg_tables >> end_task


