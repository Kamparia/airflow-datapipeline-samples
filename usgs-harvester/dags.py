import requests
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def get_events_from_api(**context):
    events = []
    response = requests.get(Variable.get("USGS_API_URL"))
    for event in response.json().get("features", []):
        timestamp = event["properties"]["time"]/1000
        date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "event_id": event["id"],
            "event_name": event["properties"]["title"],
            "magnitude": float(event["properties"]["mag"]),
            "longitude": float(event["geometry"]["coordinates"][0]),
            "latitude": float(event["geometry"]["coordinates"][1]),
            "date": date
        }
        if float(event["properties"]["mag"]) >= 5:
            events.append(data)
    
    # push events data to next task instance using XCom
    context["ti"].xcom_push(key="events", value=events)


def save_events_to_db(**context):
    insert_query = """
        INSERT INTO public.earthquake_events (event_id, event_name, magnitude, longitude, latitude, date)
        VALUES (%s, %s, %s, %s, %s, %s); 
        """
    # pull event data from previous task instance using XCom
    events = context["ti"].xcom_pull(task_ids='get_new_events', key='events')
    for event in events:
        params = tuple(event.values())
        PostgresHook(postgres_conn_id="postgres_default", schema="usgs_eq").run(insert_query, parameters=params)


# dags default arguments
default_args = {
    "owner": "me",
    "start_date": datetime(2020, 10, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),    
    "schedule_interval": "@daily"
}

# instantiate aiflow dag
with DAG('usgs_harvester', default_args=default_args, schedule_interval='0 8 * * *') as dag:
    # Task One: Create Postgres Table (if none exists).
    task_one = PostgresOperator(
        task_id = 'create_table',
        sql = '''CREATE TABLE IF NOT EXISTS public.earthquake_events (
            event_id VARCHAR(50) NOT NULL,
            event_name VARCHAR(160) NOT NULL,
            magnitude DECIMAL NOT NULL,
            longitude DECIMAL NOT NULL,
            latitude DECIMAL NOT NULL,
            date DATE NOT NULL);''',
        postgres_conn_id = 'postgres_default',
        database = Variable.get("USGS_DB_NAME")
    )
    
    # Task 2: Requests new events data from the USGS Earthquake API.
    task_two = PythonOperator(
        task_id = 'get_new_events',
        python_callable = get_events_from_api,
        provide_context = True
    )

    # Task 3: Store the new events data in Postgres.
    task_three = PythonOperator(
        task_id = 'save_events_to_db',
        python_callable = save_events_to_db,
        provide_context = True
    )

    # Task 4: Send Slack notifications to team members.
    task_four = SlackWebhookOperator(
        task_id = 'send_slack_notification',
        http_conn_id = 'slack_conn',
        webhook_token = Variable.get("SLACK_API_TOKEN"),
        message = "New earthquake events was successfully harvested.",
        username = 'airflow'
    )

    # execute pipeline tasks in series
    task_one >> task_two >> task_three >> task_four