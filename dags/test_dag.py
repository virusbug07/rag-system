from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="example_hello_world",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow DAG!'"
    )
