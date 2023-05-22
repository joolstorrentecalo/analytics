import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    BAMBOOHR_API_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

affinity = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {"key": "test", "operator": "In", "values": ["true"]}
                    ]
                }
            ]
        }
    }
}
tolerations = [
    {"key": "test", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
]

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

logging.info(pod_env_vars)
# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 5, 10),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "bamboohr_extract",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    concurrency=1,
)

# BambooHR Extract
bamboohr_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python bamboohr/src/execute.py
"""

# having both xcom flag flavors since we're in an airflow version where one is being deprecated
bamboohr_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="bamboohr-extract",
    name="bamboohr-extract",
    secrets=[
        BAMBOOHR_API_TOKEN,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    affinity=affinity,
    tolerations=tolerations,
    arguments=[bamboohr_extract_cmd],
    do_xcom_push=True,
    dag=dag,
)
