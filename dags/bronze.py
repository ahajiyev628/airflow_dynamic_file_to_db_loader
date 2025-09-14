from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import shutil

LOCAL_DIR = "/opt/airflow/files/data"
FILENAME = "card_transactions.csv"

BUCKET_NAME = "bronze"
MINIO_CONN_ID = "minio_conn"

ARCHIVE_DIR = "/opt/airflow/files/archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def upload_to_bronze(**context):
    file_path = os.path.join(LOCAL_DIR, FILENAME)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3.load_file(
        filename=file_path,
        key=f"bronze/{FILENAME}",
        bucket_name=BUCKET_NAME,
        replace=True
    )
    print(f"Uploaded {file_path} to MinIO bucket '{BUCKET_NAME}'")


def archive_local_file(**context):
    src_path = os.path.join(LOCAL_DIR, FILENAME)
    dst_path = os.path.join(ARCHIVE_DIR, FILENAME)

    if os.path.exists(src_path):
        shutil.move(src_path, dst_path)
        print(f"Moved {src_path} → {dst_path}")
    else:
        print(f"{src_path} not found, skipping archive")

with DAG(
    dag_id="bronze_layer_ingestion",
    start_date=datetime(2025, 9, 14),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_conn",
        filepath=FILENAME, 
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    move_to_bronze = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_bronze,
    )

    archive_file = PythonOperator(
        task_id="archive_local_file",
        python_callable=archive_local_file,
    )

    wait_for_file >> move_to_bronze >> archive_file
