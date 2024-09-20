"""
Run Level Up extract for `cursor` based endpoints

Because it's cursor based endpoint, one task can be used to run
all the data from that endpoint, so catchup is set to False

To backfill, the metadata database value needs to be updated manually, more info in handbook
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
    GITLAB_METADATA_DB_HOST,
    GITLAB_METADATA_DB_PASS,
    GITLAB_METADATA_DB_USER,
    GITLAB_METADATA_PG_PORT,
    LEVEL_UP_METADATA_DB_NAME,
    LEVEL_UP_METADATA_SCHEMA,
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
    "el_level_up_thought_industries_cursor_endpoints",
    default_args=default_args,
    # Run daily at 6:00am UTC, prior to the dbt run
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 8, 28),
    catchup=False,
    concurrency=2,  # num of max_tasks, limit due to API rate limiting
)

endpoint_classes = (
    "Users",
    "Content",
    "Meetings",
    "Clients",
    "AssessmentAttempts",
    "Coupons",
)
extract_tasks = []

for endpoint_class in endpoint_classes:
    extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        f"""python level_up_thought_industries/src/execute.py \
        execute-cursor-endpoint --class-name-to-run={endpoint_class}"""
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
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            LEVEL_UP_METADATA_DB_NAME,
            LEVEL_UP_METADATA_SCHEMA,
            LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY,
        ],
        env_vars={
            **pod_env_vars,
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
        },
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[extract_command],
        dag=dag,
    )
    extract_tasks.append(extract_task)
