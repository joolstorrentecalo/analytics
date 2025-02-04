"""
Quarterly Dag for Sales Analytics notebooks
"""

import os
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    clone_repo_cmd,
    SALES_ANALYTICS_NOTEBOOKS_PATH,
    get_sales_analytics_notebooks,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_SALES_ANALYTICS_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
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
    "start_date": datetime(2022, 10, 12),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
# Schedule to run quarterly on the 7th day of the quarter at 6AM
dag = DAG(
    "sales_analytics_quarterly_notebooks",
    default_args=default_args,
    schedule_interval="0 6 7 */3 *",
    concurrency=1,
    catchup=False,
)


notebooks = get_sales_analytics_notebooks(frequency="quarterly")

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
    sales_analytics_quarterly_notebooks = KubernetesPodOperator(
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
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity("sales_analytics"),
        tolerations=get_toleration("sales_analytics"),
        arguments=[container_cmd_load],
        dag=dag,
    )
    start >> sales_analytics_quarterly_notebooks
