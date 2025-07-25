from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
  "owner": "airflow"
}

def hello_world():
  print("Hello world!")

def hello_airflow():
  print("Hellow airflow!")

with DAG (
  dag_id = "test_dag",
  description = "Demo dag",
  schedule_interval = "5 0 * * *",
  default_args = default_args,
  start_date = datetime(2025,7,7)
) as dag:
  start_task = DummyOperator(task_id="start_task")
  end_task = DummyOperator(task_id="end_task")

  hello_world_task = PythonOperator(task_id="hello_world_task",
                                     python_callable = hello_world)

  hello_airflow_task = PythonOperator(task_id="hello_airflow_task",
                                     python_callable = hello_airflow)

  start_task >> [hello_world_task, hello_airflow_task] >> end_task
