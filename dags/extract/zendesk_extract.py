"""
Run daily Zendesk extract for tickets and ticket_audits tables
"""
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
    SNOWFLAKE_LOAD_WAREHOUSE,
    ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
    GCP_SERVICE_CREDS,
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
    "retries": 2,
    "description": "This DAG is extracts and transforms sensitive zendesk tickets and tickets_audits tables",
}

# Define the DAG
dag = DAG(
    f"tl_zendesk_sensitive",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 11, 7),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

zendesk_ticket_audits_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python zendesk/src/zendesk_sensitive_extraction.py refactor_ticket_audits"
)

zendesk_tickets_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python zendesk/src/zendesk_sensitive_extraction.py refactor_tickets"
)

zendesk_extract_ticket_audits_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=f"tl-zendesk-ticket-audits-extract-daily",
    name=f"tl-zendesk-ticket-audits-extract-daily",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
        GCP_SERVICE_CREDS,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction_highmem"),
    tolerations=get_toleration("extraction_highmem"),
    arguments=[zendesk_ticket_audits_extract_command],
    dag=dag,
)

zendesk_extract_tickets_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=f"tl-zendesk-tickets-extract-daily",
    name=f"tl-zendesk-tickets-extract-daily",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
        GCP_SERVICE_CREDS,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction_highmem"),
    tolerations=get_toleration("extraction_highmem"),
    arguments=[zendesk_tickets_extract_command],
    dag=dag,
)

zendesk_extract_ticket_audits_task
zendesk_extract_tickets_task
