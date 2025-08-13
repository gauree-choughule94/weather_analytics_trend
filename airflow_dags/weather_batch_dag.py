from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_weather_main():
    script_path = "/home/neosoft/Desktop/wheather_analytics_trends/pyspark_jobs/main.py" 

    print(f"Running Python script: {script_path}")
    result = subprocess.run(
        ["python3", script_path],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Script failed with exit code {result.returncode}")

# Default Airflow args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_main_script_runner",
    default_args=default_args,
    description="Run main.py from pyspark_jobs using PythonOperator",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 13),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "pyspark"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    run_main_task = PythonOperator(
        task_id="run_main_py",
        python_callable=run_weather_main,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Task dependencies
    start_pipeline >> run_main_task >> end_pipeline
