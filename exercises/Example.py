import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#
# TODO: Define a function for the PythonOperator to call and have it log something
#
def greet():
    logging.info('Hello World Engineers!')

def second_greet():
    logging.info('Hello Data Scientists!')

dag = DAG(
    'session1.exercise32',
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

#
# TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#
greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)

second_greet_task = PythonOperator(
    task_id="second_greet",
    python_callable=second_greet,
    dag=dag
)

greet_task >> second_greet_task