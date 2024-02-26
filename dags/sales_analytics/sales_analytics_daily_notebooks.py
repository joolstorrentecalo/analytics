"""
Daily Dag for Sales Analytics notebooks
"""


import os
from datetime import datetime, timedelta
import pathlib
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    clone_repo_cmd,
    get_sales_analytics_notebooks,
    SALES_ANALYTICS_NOTEBOOKS_PATH,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_SALES_ANALYTICS_LOAD_ROLE,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
    GSHEETS_SERVICE_ACCOUNT_CREDENTIALS,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 7, 25),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
# Schedule to run daily at 6PM
dag = DAG(
    "sales_analytics_daily_notebooks",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    concurrency=1,
    catchup=False,
)

notebooks = get_sales_analytics_notebooks(frequency="daily")

# Task 1
start = DummyOperator(task_id="Start", dag=dag)

for notebook, task_name in notebooks.items():
    absolute_path = pathlib.Path(SALES_ANALYTICS_NOTEBOOKS_PATH) / notebook
    notebook_parent = absolute_path.parent.as_posix()
    notebook_filename = absolute_path.name

    # Set the command for the container for loading the data
    container_cmd_load = f"""
        {clone_repo_cmd} &&
        cd {notebook_parent} &&
        papermill {notebook_filename} -p is_local_development False
        """
    task_identifier = f"{task_name}"
    # Task 2
    sales_analytics_daily_notebooks = KubernetesPodOperator(
        **gitlab_defaults,
        image=ANALYST_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        pool="default_pool",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_SALES_ANALYTICS_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            GITLAB_ANALYTICS_PRIVATE_TOKEN,
            GSHEETS_SERVICE_ACCOUNT_CREDENTIALS,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity("sales_analytics"),
        tolerations=get_toleration("sales_analytics"),
        arguments=[container_cmd_load],
        dag=dag,
    )
    start >> sales_analytics_daily_notebooks
