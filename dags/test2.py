# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER URLS')

# 2. Get team URLS
def team_urls():
    # Empty list for team urls
    team_urls = []

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='datalake1'
    )

    # SQL Statement
    sql_statement = "SELECT team_url FROM team_urls"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_statement)

    # Extract team URLs from Data Lake
    for row in cursor.fetchall():
        team_urls.append(row[0])

    return team_urls

# 3. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,OBTAINED EPL TEAM URLS')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

dag = DAG('scrape_player_urls_DAG',
          schedule_interval = '00 05 * * *',
          catchup = False,
          default_args = default_args)

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Retrieve team urls from data lake
get_team_urls_task = PythonOperator(
    task_id = "get_team_urls_task",
    python_callable = team_urls,
    do_xcom_push = True,
    dag = dag
)

# 3. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_team_urls_task >> end_task
