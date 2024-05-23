import logging
import os
from datetime import datetime, timedelta
import yaml
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    REPO_BASE_PATH,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    HYPERPROOF_CLIENT_ID,
    HYPERPROOF_CLIENT_SECRET,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

logging.info(pod_env_vars)
# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 5, 20),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "hyperproof_extract",
    default_args=default_args,
    concurrency=2,
    schedule_interval="25 22 * * *",
    catchup=False,
)


extract_command = f"""
    {clone_and_setup_extraction_cmd} &&
    python hyperproof/extract_hyperproof.py 
"""


# having both xcom flag flavors since we're in an airflow version where one is being deprecated
hyperproof_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="hyperproof_extract",
    name="hyperproof_extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        HYPERPROOF_CLIENT_ID,
        HYPERPROOF_CLIENT_SECRET,
    ],
    env_vars={
        **pod_env_vars,
        "TASK_INSTANCE": "{{ task_instance_key_str }}",
        "START_TIME": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[extract_command],
    dag=dag,
)
