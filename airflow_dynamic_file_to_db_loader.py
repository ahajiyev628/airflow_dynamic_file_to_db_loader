import os
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Join Zoom Meeting 
# https://ibar.zoom.us/j/96932852138?pwd=Wsdm6HQj5QpPeEsnlU4NH2IpAcxBKh.1 
# Meeting ID: 969 3285 2138 
# Passcode: 187990 


FILES_DIRECTORY = "./files/test/"

def execute_postgres_query(query, parameters=None):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook.run(query, parameters=parameters)


def log_initial_task_details(context):
    params = context['params']  # ✅ Corrected
    query = """
        INSERT INTO airflow_daily_log (log_time, source_name, target_name)
        VALUES (CURRENT_DATE, %(source)s, %(target)s)
        ON CONFLICT (log_time, source_name, target_name) DO NOTHING
    """
    values = {
        'source': params['source_file_name'],
        'target': params['target_table_name']
    }

    execute_postgres_query(query, values)


def merge_task_state(context, state):
    ti = context['task_instance']
    params = context['params']  # ✅ Corrected

    query = """
        UPDATE airflow_daily_log
        SET
            status = %(status)s,
            dag_id = %(dag_id)s,
            task_id = %(task_id)s,
            run_id = %(run_id)s,
            execution_date = %(execution_date)s,
            try_number = %(try_number)s,
            log_time = CURRENT_TIMESTAMP
        WHERE report_date = CURRENT_DATE
          AND source = %(source)s
          AND target = %(target)s
    """
    values = {
        'status': state,
        'dag_id': ti.dag_id,
        'task_id': ti.task_id,
        'run_id': ti.run_id,
        'execution_date': str(context['execution_date']),
        'try_number': ti.try_number,
        'source': params['source_file_name'],
        'target': params['target_table_name']
    }

    execute_postgres_query(query, values)


def task_running_callback(context):
    log_initial_task_details(context)
    merge_task_state(context, state="running")


def task_success_callback(context):
    merge_task_state(context, state="success")


def task_failure_callback(context):
    merge_task_state(context, state="failed")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 11),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id="DYNAMIC_FILE_TO_DB_LOADER",
    description='Example DAG with dynamic task mapping',
    default_args=default_args,
    schedule_interval="*/20 0-12 * * *",
    concurrency=16,
    max_active_runs=2,
    catchup=False,
    tags=['DE']
) as dag:

    @task
    def list_minio_objects():
        hook = S3Hook(aws_conn_id="minio_conn")
        bucket_name = "ahajiyev"
        keys = hook.list_keys(bucket_name=bucket_name)
        print(f"Objects in {bucket_name}: {keys}")
        return keys

    minio_objects = list_minio_objects()

    @task
    def get_required_files_and_download():
        file_mappings = json.loads(Variable.get("VAR_FILE_TO_DB"))

        hook = S3Hook(aws_conn_id="minio_conn")
        bucket_name = "ahajiyev"

        os.makedirs(FILES_DIRECTORY, exist_ok=True)
        downloaded_mappings = []

        for mapping in file_mappings:
            source_file = mapping["source_file_name"]
            if hook.check_for_key(source_file, bucket_name):
                downloaded_tmp = hook.download_file(
                    key=source_file,
                    bucket_name=bucket_name,
                    local_path=FILES_DIRECTORY
                )
                final_path = os.path.join(FILES_DIRECTORY, source_file)
                os.rename(downloaded_tmp, final_path)
                downloaded_mappings.append(mapping)
            else:
                logging.warning(f"File {source_file} not found in bucket {bucket_name}")

        logging.info(f"Downloaded Mappings: {downloaded_mappings}")
        return downloaded_mappings


    needed_files = get_required_files_and_download()

    transfer_task = KubernetesPodOperator.partial(
        task_id="transfer_files",
        name="excel-to-parquet",
        namespace="default",
        image="ahajiyev/pyspark-with-awscli",
        cmds=["sh", "-c"],
        arguments=[
            (
                "aws s3 --endpoint-url http://minio.default.svc.cluster.local:9000 "
                "cp s3://ahajiyev/dynamic_file_to_db_transfer.py /tmp/script.py && "
                "python /tmp/script.py --source {{ params.source_file_name }} --target ahajiyev/{{ params.target_table_name }}"
            )
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": "adminic",
            "AWS_SECRET_ACCESS_KEY": "adminic123",
            "PYSPARK_PYTHON": "python3"
        },
        get_logs=True,
        is_delete_operator_pod=False,
        on_execute_callback=task_running_callback,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    ).expand(
        params=needed_files
    )


    minio_objects >> needed_files >> transfer_task
