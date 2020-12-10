import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# callable python function
def print_world():
    print('world')


# dags default arguments
default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2017, 6, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# instantiate aiflow dag
with DAG('airflow_tutorial_v01', default_args=default_args, schedule_interval='0 * * * *') as dag:
    # Task 1 - print "hello"
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello"'
    )

    # Task 2 - sleep for 5 secs
    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    # Task 3 - print "world"
    print_world = PythonOperator(
        task_id='print_world',
        python_callable=print_world
    )

# execute pipeline tasks in series
print_hello >> sleep >> print_world