""" Airflow DAG for loading Zuora Revenue Report from API"""
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
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2021, 6, 1),
    "dagrun_timeout": timedelta(hours=6),
}

task_name = "zuora-revenue-report"

full_path = (
    f"{REPO_BASE_PATH}/extract/zuora_revenue_report/src/zuora_report_api_list.yml"
)


def get_yaml_file(path: str) -> dict:
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
            return None


stream = get_yaml_file(path=full_path)
report_list = list(stream["report_list"].keys())


# Create the DAG  with one report at once
dag = DAG(
    "el_zuora_revenue_report",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    concurrency=1,
    catchup=False,
)

start = DummyOperator(task_id="Start", dag=dag)

for report in report_list:
    file_name_to_load = stream["report_list"][report]["output_file_name"].lower()
    body_load_query_string = stream["report_list"][report]["load_column_name"]
    report_type = stream["report_list"][report]["report_type"]
    # Set the command for the container for loading the data
    container_cmd_load = f"""
        {clone_and_setup_extraction_cmd} &&
        python3 zuora_revenue_report/load_zuora_revenue_report.py zuora_report_load --bucket zuora_revpro_gitlab\
                --schema zuora_revenue_report --output_file_name {file_name_to_load}\
                --body_load_query "{body_load_query_string}" --report_type {report_type}
        """
    task_identifier = f"el-{task_name}-{report.replace('_','-').lower()}-load"
    # Task 2
    zuora_revenue_load_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        pool="default_pool",
        secrets=[
            GCP_SERVICE_CREDS,
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
    start >> zuora_revenue_load_run
