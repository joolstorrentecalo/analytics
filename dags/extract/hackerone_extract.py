"""
Run Hackerone API export. The API returns all the data for each report
for that particular point in time.

The DAG can run backfill from a default start date when run with the parameter is_full_refresh=True.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models.param import Param
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    HACKERONE_API_USERNAME,
    HACKERONE_API_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration


pod_env_vars = {**gitlab_pod_env_vars, **{}}

params = {
    "is_full_refresh": Param(
        False,
        type="boolean",
    )
}

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}

# Define the DAG
dag = DAG(
    "el_hackerone_extract",
    description="Extract data from hackerone API reports endpoint",
    default_args=default_args,
    # Run shortly before dbt dag which is at 8:45UTC
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 8, 27),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    params=params,
)

hackerone_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python hackerone/src/hackerone_get_reports.py"
)

hackerone_task_name = "hackerone_get_reports"

hackerone_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id=hackerone_task_name,
    name=hackerone_task_name,
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        HACKERONE_API_USERNAME,
        HACKERONE_API_TOKEN,
    ],
    env_vars={
        **pod_env_vars,
        "is_full_refresh": "{{params.is_full_refresh}}",
        "execution_date": "{{execution_date}}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[hackerone_extract_command],
    dag=dag,
)

hackerone_task
