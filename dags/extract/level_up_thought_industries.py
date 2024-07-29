"""
Run daily Level Up extract
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
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
    "el_level_up_thought_industries",
    default_args=default_args,
    # daily 1:00 UTC: wait one hour as buffer before running previous day
    schedule_interval="0 1 * * *",
    # FYI: on first run, data is backfilled thru 2022-03-01
    start_date=datetime(2022, 3, 1),
    catchup=True,
    max_active_runs=1,  # due to API rate limiting
    concurrency=3,  # num of max_tasks, limit due to API rate limiting
)

endpoint_classes = (
    "CourseActions",
    "CourseCompletions",
    "CourseViews",
    "Logins",
    "Visits",
)
extract_tasks = []

for endpoint_class in endpoint_classes:
    extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        f"python level_up_thought_industries/src/execute.py --class_name_to_run={endpoint_class}"
    )

    extract_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE_3_10,
        task_id=f"el-{endpoint_class}",
        name=f"el-{endpoint_class}",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY,
        ],
        env_vars={
            **pod_env_vars,
            # remove time from logical_date, and convert to epoch timestamp
            "EPOCH_START_STR": (
                "{{ logical_date.replace(hour=0, minute=0, second=0, microsecond=0)"
                ".int_timestamp }}"
            ),
            "EPOCH_END_STR": (
                "{{ next_execution_date.replace(hour=0, minute=0, second=0, microsecond=0)"
                ".int_timestamp }}"
            ),
        },
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[extract_command],
        dag=dag,
    )
    extract_tasks.append(extract_task)
