
import datetime
import logging

from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


def start():
    logging.info('Starting the DAG')


def load_data_to_rds(*args, **kwargs):
    aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='s3')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    rds_hook = PostgresHook(postgres_conn_id='rds', schema='datalake2')
    rds_hook.run(sql_statements.COPY_ALL_JOB_TITLES_SQL.format(credentials.access_key, credentials.secret_key, credentials.token))


dag = DAG(
    'session2.exercise5',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
            CREATE TABLE IF NOT EXISTS job_titles (job_title VARCHAR(100),language VARCHAR(100),suspended VARCHAR(100));
        '''
)

copy_task = PythonOperator(
    task_id='load_from_s3_to_rds',
    dag=dag,
    python_callable=load_data_to_rds
)

data_validation = PostgresOperator(
    task_id="results_validation_summary",
    dag=dag,
    postgres_conn_id='rds',
    sql=sql_statements.VALIDATE_RESULTS
)


greet_task >> create_table >> copy_task >>data_validation
