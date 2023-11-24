# import 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Ivan Manuel',
    'start_date': days_ago(0),
    'email': ['ivanmnlw@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define DAG
dag = DAG(
    dag_id = 'spotify_dag',
    default_args = default_args,
    description = 'Simple ETL Spotify',
    schedule_interval = timedelta(days=1),
)

run_etl = BashOperator(
    task_id = 'whole_spotify_etl',
    bash_command = '/mnt/c/Users/ASUS/Desktop/ONLINE_COURSE/Data_Engineering/Spotify_API/command.sh ',
    dag = dag,
)

run_etl