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
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    OCI_FINGERPRINT,
    OCI_KEY_CONTENT,
    OCI_REGION,
    OCI_TENANCY,
    OCI_USER,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 8, 24),
}

dag = DAG(
    "el_oci_usage",
    default_args=default_args,
    schedule_interval="25 3 * * *",
    concurrency=2,
)

container_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python oci_usage/extract.py
"""

oci_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="oci_usage_extract",
    name="oci_usage_extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
        OCI_FINGERPRINT,
        OCI_KEY_CONTENT,
        OCI_REGION,
        OCI_TENANCY,
        OCI_USER,
    ],
    env_vars={
        **pod_env_vars,
        **{
            "START": "{{ execution_date.isoformat() }}",
            "END": "{{ next_execution_date.isoformat() }}",
        },
    },  # merge the dictionaries into one
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[container_cmd],
    dag=dag,
)
