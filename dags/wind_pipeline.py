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
    dag_id='wind_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['kmi', 'ecmwf', 'kaggle-ukkel'],
) as dag:

    fetch_kmi = PythonOperator(
        task_id='fetch_kmi_data',
        python_callable=lambda: _run_script('/app/scripts/GEO.py'),
    )

    fetch_ecmwf = PythonOperator(
        task_id='fetch_ecmwf_data',
        python_callable=lambda: _run_script('/app/scripts/ECMWF.py'),
    )

    fetch_ukkel = PythonOperator(
        task_id='fetch_ukkel_data',
        python_callable=lambda: _run_script('/app/scripts/kaggleUkkel.py'),
    )

    combine = PythonOperator(
        task_id='wind',
        python_callable=lambda: _run_script('/app/scripts/wind.py'),
    )

    # Fetch tasks run independently, combine runs after all three succeed
    [fetch_kmi, fetch_ecmwf, fetch_ukkel] >> combine