import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kube_secrets import (
    GRAPHITE_HOST,
    GRAPHITE_PASSWORD,
    GRAPHITE_USERNAME,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{"START_DATE": "{{ ds_nodash }}"}}

default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    # "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    # Want to extract the last year
    "start_date": datetime(2023, 8, 24),
}

dag = DAG(
    "lcp_extract",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    catchup=True,
)


# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
extract_command = (
    f"{clone_and_setup_extraction_cmd} && python graphite/extract_graphite.py"
)

lcp_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="lcp-extract",
    name="lcp-extract",
    secrets=[
        GRAPHITE_HOST,
        GRAPHITE_PASSWORD,
        GRAPHITE_USERNAME,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[extract_command],
    env_vars=pod_env_vars,
    dag=dag,
)
