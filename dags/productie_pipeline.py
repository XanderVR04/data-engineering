from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'gemini',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _run_script(path: str, timeout: int = 1200):
    script = os.path.abspath(path)
    print(f"Running script: {script}")
    subprocess.run([sys.executable, script], check=True, timeout=timeout)


with DAG(
    dag_id='productie',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['productie'],
) as dag:

    load_csv = PythonOperator(
        task_id='productie',
        python_callable=lambda: _run_script('/app/scripts/data-scripts/productie.py'),
    )
