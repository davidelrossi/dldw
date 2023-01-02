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
        postgres_conn_id='dw_stage'
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
        postgres_conn_id='dw_stage'
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
        postgres_conn_id='dw_stage'
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
        postgres_conn_id='dw_stage'
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
    # Change data type for id in elements dataframe
    df_el['id'] = df_el['id'].astype(int)

    # Merge gameweek dataframe with elements dataframe on "id"
    df = df.merge(df_2, left_on='element', right_on='id', how='left')



