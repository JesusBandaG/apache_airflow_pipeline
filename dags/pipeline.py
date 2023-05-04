from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta, datetime
from pandas_profiling import ProfileReport
import pytz
import pandas as pd

URL = 'https://raw.githubusercontent.com/JesusBandaG/apache_airflow_pipeline/main/titanic.csv'
PATH = '/opt/airflow/dags/data/titanic.csv'


DEFULT_ARGS = {
    'owner': 'Jesus Banda',
    'retries': 2,
    'retry_delay': timedelta(minutes=0.5)
}

with DAG(
    'dag_data_pipeline',
    description = 'Data pipeline execution',
    default_args = DEFULT_ARGS,
    catchup = False,
    start_date = datetime(2023, 1, 1),
    schedule_interval = '@once',
    tags = ['data_pipeline', 'docker_postgres']
) as dag:
    download_data = BashOperator(
        task_id = 'download_file',
        bash_command = 'curl -o {{ params.path }}  {{ params.url }}',
        params = {
            'path': PATH,
            'url': URL
        }
    )