"""
saas_usage_ping.py is responsible for orchestrating:
- usage ping combined metrics (sql + redis)
- usage ping namespace
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

NUMBER_OF_TASKS = 30

# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
# ds - the execution date as YYYY-MM-DD
pod_env_vars = {
    "RUN_DATE": "{{ next_ds }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
    "SNOWFLAKE_LOAD_WAREHOUSE": "USAGE_PING",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}


secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
]

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=8),
}

DAG_DESCRIPTION = (
    "This DAG run to calculate 2 types of metrics: "
    "1) instance_combined_metrics and "
    "2) instance_namespace_metrics "
)

# Create the DAG
#  Monday at 0700 UTC
dag = DAG(
    "saas_usage_ping",
    default_args=default_args,
    concurrency=5,
    description=DAG_DESCRIPTION,
    schedule_interval="0 7 * * 1",
    catchup=False,
)

# Instance Level Usage Ping
instance_combined_metrics_cmd = f"""
    {clone_repo_cmd} &&
    cd analytics/extract/saas_usage_ping/ &&
    python3 transform_postgres_to_snowflake.py &&
    python3 usage_ping.py saas_instance_combined_metrics
"""

instance_combined_metrics_ping = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="saas-instance-usage-ping-combined-metrics",
    name="saas-instance-usage-ping-combined-metrics",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[instance_combined_metrics_cmd],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    dag=dag,
)

dummy_start_namespace = DummyOperator(
    task_id="start_saas-namespace-usage-ping", dag=dag
)


def get_task_name(current_chunk: int, number_of_tasks: int) -> str:
    """
    Generate task name
    """

    return f"saas-namespace-usage-ping-chunk-{current_chunk:02}-{number_of_tasks:02}"


def generate_command(chunk_no: int, number_of_tasks: int):
    """
    Generate command to run instance_namespace_metrics (per chunk)
    """
    # Namespace, Group, Project, User Level Usage Ping
    return f"""
        {clone_repo_cmd} &&
        cd analytics/extract/saas_usage_ping/ &&
        python3 instance_namespace_metrics.py saas_instance_namespace_metrics --ping_date=$RUN_DATE --chunk_no={chunk_no} --number_of_tasks={number_of_tasks}
    """


def generate_namespace_task(
    current_chunk: int, number_of_tasks: int
) -> KubernetesPodOperator:
    """
    Generate tasks for namespace
    """
    task_id = task_name = get_task_name(
        current_chunk=current_chunk, number_of_tasks=number_of_tasks
    )

    namespace_command = generate_command(
        chunk_no=current_chunk, number_of_tasks=number_of_tasks
    )

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_id,
        name=task_name,
        secrets=secrets,
        env_vars=pod_env_vars,
        arguments=[namespace_command],
        dag=dag,
        retries=2,
        affinity=get_affinity("extraction_highmem"),
        tolerations=get_toleration("extraction_highmem"),
    )


[instance_combined_metrics_ping]


for i in range(1, NUMBER_OF_TASKS + 1):
    dummy_start_namespace >> generate_namespace_task(
        current_chunk=i, number_of_tasks=NUMBER_OF_TASKS
    )
