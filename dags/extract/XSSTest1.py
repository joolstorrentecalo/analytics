# File path: /path/to/your/airflow/dags/test_xss_vulnerability.py

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the XSS payload
xss_payload = "<script>alert('XSS Vulnerability Detected!');</script>"

# Create the DAG
with DAG(
    dag_id='test_xss_vulnerability',
    default_args=default_args,
    description='A simple test for XSS vulnerability in Airflow',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example', xss_payload],  # Injecting the XSS payload here
) as dag:

    # Define tasks
    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    # Set task dependencies
    start >> end
