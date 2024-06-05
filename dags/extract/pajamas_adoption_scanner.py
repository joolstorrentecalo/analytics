"""
Run daily pajamas adoption scanner extract
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from kubernetes_helpers import get_affinity, get_toleration

pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}

# Define the DAG
dag = DAG(
    "el_pajamas_adoption_scanner_extract",
    default_args=default_args,
    # Run shortly before dbt dag which is at 8:45UTC
    schedule_interval="15 8 * * *",
    start_date=datetime(2024, 1, 23),
    catchup=False,
    max_active_runs=1,
)

pajamas_adoption_scanner_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python pajamas_adoption_scanner/src/pajamas_adoption_scanner.py"
)

pajamas_adoption_scanner_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id="pajamas_adoption_scanner-extract",
    name="pajamas_adoption_scanner-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars={
        **pod_env_vars,
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[pajamas_adoption_scanner_extract_command],
    dag=dag,
)

pajamas_adoption_scanner_task
