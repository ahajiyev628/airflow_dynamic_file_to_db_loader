from airflow import Dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
}

def test():
    print("Hello World!")


with Dag (
    dag_id = "test_dag",
    description = "sample dag",
    default_args = default_args,
    schedule_interval = "5 8 * * *",
    start_date = datetime(2025, 4, 11),
    catchup = False
) as dag:
    start_task = DummyOperator("start_task")

    hello_task = PythonOperator("hello_task",
                                python_collable = test)
    
    end_task = DummyOperator("end_task")

    start_task >> hello_task >> end_task





