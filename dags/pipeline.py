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

TZ = pytz.timezone('America/Mexico_City')
TODAY = datetime.now(TZ).strftime('%Y-%m-%d')

URL = 'https://raw.githubusercontent.com/JesusBandaG/apache_airflow_pipeline/main/titanic.csv'
PATH = '/opt/airflow/dags/data/titanic.csv'
OUTPUT_DQ = '/opt/airflow/dags/data/data_quality_report_{}.html'.format(TODAY)
OUTPUT_SQL = '/opt/airflow/dags/sql/titanic_{}.sql'.format(TODAY)
OUTPUT = '/opt/airflow/dags/data/titanic_curated_{}.csv'.format(TODAY)
TARGET = 'sql/titanic_{}.sql'.format(TODAY)


DEFULT_ARGS = {
    'owner': 'Jesus Banda',
    'retries': 2,
    'retry_delay': timedelta(minutes=0.5)
}

def _profile():
    df = pd.read_csv(PATH)
    profile = ProfileReport(df, title='Data Quality Report')
    profile.to_file(OUTPUT_DQ)

def _curate():
    # Read CSV file
    df = pd.read_csv(PATH)
    
    # Remove unnecesary columns
    df.drop(['Ticket', 'Cabin'], axis=1, inplace=True)

    # Apply some techniques to fill null values
    df['Age'].fillna(df['Age'].median(), inplace=True)
    df['Embarked'].fillna(df['Embarked'].mode()[0], inplace=True)

    # Clean 'Name' column to just have the full name
    df['Full Name'] = df['Name'].apply(lambda x: ' '.join(x.split(',')[1].split('(')[0].strip().split(' ')[1:]) + ' ' + x.split(',')[0])
    df['Full Name'] = df['Full Name'].str.replace('["\']', '', regex=True)

    # Add a new column for the 'Title' taken from the 'Name' column
    df['Title'] = df['Name'].apply(lambda x: x.split(',')[1].split('.')[0].strip())

    # Remove 'Name' column
    df.drop(['Name'], axis=1, inplace=True)

    # Map titles to reduce variability of that field
    title_dict = {
        'Capt': 'Officer',
        'Col': 'Officer',
        'Major': 'Officer',
        'Jonkheer': 'Royalty',
        'Don': 'Royalty',
        'Sir': 'Royalty',
        'Dr': 'Officer',
        'Rev': 'Offcier',
        'the Countess': 'Royalty',
        'Mme': 'Mrs',
        'Mlle': 'Miss',
        'Ms': 'Mrs',
        'Mr': 'Mr',
        'Mrs': 'Mrs',
        'Miss': 'Miss',
        'Master': 'Master',
        'Lady': 'Royalty'
    }
    df['Title'] = df['Title'].map(title_dict)

    # Reordering the columns as desired
    df = df[['PassengerId', 'Full Name', 'Title', 'Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked']]

    # Save file in CSV format
    df.to_csv(OUTPUT, index=False)

    # Iterate through all the rows and insert them
    with open(OUTPUT_SQL, 'w') as f:
        for index, row in df.iterrows():
            values = f"({row['PassengerId']}, '{row['Full Name']}', '{row['Title']}', {row['Survived']}, {row['Pclass']}, '{row['Sex']}', {row['Age']}, {row['SibSp']}, {row['Parch']}, {row['Fare']}, '{row['Embarked']}')"
            insert = f"INSERT INTO raw_titanic VALUES {values};\n"
            f.write(insert)

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

    with TaskGroup("DataQuality", tooltip="Quality Data Engine") as dq:
        profiling = PythonOperator(
            task_id = 'profiling',
            python_callable = _profile
        )

        curating = PythonOperator(
            task_id = 'curating',
            python_callable = _curate
        )

        profiling >> curating

    with TaskGroup("RawLayer", tooltip="Raw Layer") as raw_layer:
        create_raw = PostgresOperator(
            task_id = 'create_raw',
            postgres_conn_id = 'postgres_docker',
            sql = '''
                CREATE TABLE IF NOT EXISTS raw_titanic (
                    passenger_id VARCHAR(50),
                    full_name VARCHAR(50),
                    title VARCHAR(50),
                    survived INT,
                    pclass INT,
                    sex VARCHAR(10),
                    age FLOAT,
                    sibsp INT,
                    parch INT,
                    fare FLOAT,
                    embarked VARCHAR(10)
                )
            '''
        )

        load_raw = PostgresOperator(
            task_id = 'load_raw',
            postgres_conn_id = 'postgres_docker',
            sql = TARGET
        )

        create_raw >> load_raw

    with TaskGroup("MasterLayer", tooltip="Master Layer") as master_layer:
        create_master = PostgresOperator(
            task_id = 'create_master',
            postgres_conn_id = 'postgres_docker',
            sql = '''
                CREATE TABLE IF NOT EXISTS master_titanic (
                    id VARCHAR(50),
                    full_name VARCHAR(50),
                    title VARCHAR(10),
                    survived VARCHAR(50),
                    age VARCHAR(10),
                    generation VARCHAR(20),
                    genre VARCHAR(10),
                    sibling_spouse VARCHAR(10),
                    parent_child VARCHAR(10),
                    ticket_price VARCHAR(10),
                    embarked VARCHAR(20),
                    load_date VARCHAR(10),
                    load_datetime TIMESTAMP
                )
            '''
        )

        load_master = PostgresOperator(
            task_id = 'load_master',
            postgres_conn_id = 'postgres_docker',
            sql = '''
                INSERT INTO master_titanic (
                    id, full_name, title, survived, age, generation,
                    genre, sibling_spouse, parent_child, ticket_price, embarked, load_date, load_datetime
                )
                SELECT DISTINCT
                CAST(passenger_id AS VARCHAR) AS id,
                CAST(full_name AS VARCHAR) AS full_name,
                CAST(title AS VARCHAR) AS title,
                CASE
                    WHEN survived = 1 THEN 'SURVIVOR'
                    ELSE 'NOT SURVIVOR'
                END AS survived,
                CAST(age AS VARCHAR) AS age,
                CASE
                    WHEN age > 18 THEN 'Z Gen'
                    WHEN age BETWEEN 18 AND 39 THEN 'Millenial'
                    WHEN age BETWEEN 40 AND 59 THEN 'X Gen'
                    WHEN age BETWEEN 60 AND 79 THEN 'Baby Boomer'
                    ELSE 'Older than 80 years'
                END AS generation,
                CAST(sex AS VARCHAR) AS genre,
                CAST(sibsp AS VARCHAR) AS sibling_spouse,
                CAST(parch AS VARCHAR) AS parent_child,
                CAST(fare AS VARCHAR) AS ticket_price,
                CASE
                    WHEN embarked = 'C' THEN 'Cherbourg'
                    WHEN embarked = 'Q' THEN 'Queenstown'
                    WHEN embarked = 'S' THEN 'Southampton'
                END AS embarked,
                CAST({{ params.today }} AS VARCHAR) AS load_date,
                NOW() AS load_datetime
                FROM public.raw_titanic t
                WHERE NOT EXISTS (
                    SELECT 1 FROM master_titanic mt
                    WHERE mt.id = CAST(t.passenger_id AS VARCHAR)
                )
            ''',
            params = {'today': TODAY}
        )

        create_master >> load_master

    validator = SqlSensor(
        task_id = 'validator',
        conn_id = 'postgres_docker',
        sql = 'SELECT COUNT(*) FROM public.master_titanic WHERE load_date = CAST({{ params.today }} AS VARCHAR)',
        params = {
            'today': TODAY
        },
        timeout = 30
    )    

    download_data >> dq >> raw_layer >> master_layer >> validator

    
