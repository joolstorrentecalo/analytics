"""
Run daily Clari extract
"""

import os
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
    CLARI_API_KEY,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
TASK_SCHEDULE = "daily"
TASK_NAME_PRE = f"clari-extract-{TASK_SCHEDULE}"

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
    f"clari_extract_{TASK_SCHEDULE}",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2023, 6, 3),
    catchup=False,
    max_active_runs=1,
)

clari_extract_command = (
    f"{clone_and_setup_extraction_cmd} && " f"python clari/src/clari.py"
)

clari_task_this_quarter = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id=f"{TASK_NAME_PRE}-this-quarter",
    name=f"{TASK_NAME_PRE}-this-quarter",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        CLARI_API_KEY,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",  # run yest's quarter
        "task_schedule": TASK_SCHEDULE,
        "task_instance_key_str": "{{ task_instance_key_str }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[clari_extract_command],
    dag=dag,
)

clari_task_next_quarter = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id=f"{TASK_NAME_PRE}-next-quarter",
    name=f"{TASK_NAME_PRE}-next-quarter",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        CLARI_API_KEY,
    ],
    env_vars={
        **pod_env_vars,
        # based on yest date, get the next quarter
        "logical_date": "{{ logical_date.add(months=3) }}",
        "task_schedule": TASK_SCHEDULE,
        "task_instance_key_str": "{{ task_instance_key_str }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[clari_extract_command],
    dag=dag,
)

clari_task_this_quarter >> clari_task_next_quarter
