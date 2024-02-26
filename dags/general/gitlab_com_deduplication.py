""" Airflow DAG for removing duplicate data in gitlab.com tables"""
import os
from datetime import datetime, timedelta
import logging
from yaml import safe_load, YAMLError
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 2, 21),
    "dagrun_timeout": timedelta(hours=6),
}

full_path = f"{REPO_BASE_PATH}/extract/gitlab_deduplication/manifest_deduplication/t_gitlab_com_deduplication_table_manifest.yaml"
task_name = "t_deduplication"


def get_yaml_file(path: str):
    """
    Get all the report name for which tasks for loading
    needs to be created
    """

    with open(file=path, mode="r", encoding="utf-8") as file:
        try:
            loaded_file = safe_load(stream=file)
            return loaded_file
        except YAMLError as exc:
            logging.error(f"Issue with the yaml file: {exc}")
            raise


manifest_dict = get_yaml_file(path=full_path)
table_list = manifest_dict["tables_to_de_duped"]

# Create the DAG  with one report at once
dag = DAG(
    "t_gitlab_com_deduplication",
    default_args=default_args,
    schedule_interval="0 11 * * 0",
    concurrency=3,
    catchup=False,
)

start = DummyOperator(task_id="Start", dag=dag)

for table in table_list:
    # Set the command for the container for loading the data
    container_cmd_load = f"""
        {clone_and_setup_extraction_cmd} &&
        export SNOWFLAKE_LOAD_WAREHOUSE="LOADING_XL" &&
        python3 gitlab_deduplication/main.py deduplication  --table_name {table}
        """
    task_identifier = f"{task_name}-{table.replace('_', '-').lower()}-transform"
    # Task 2
    gitlab_deduplication_transform_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        pool="default_pool",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[container_cmd_load],
        dag=dag,
    )
    start >> gitlab_deduplication_transform_run
