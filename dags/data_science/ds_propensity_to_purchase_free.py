"""
Propensity to Purchase Free DAG
"""


import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    data_test_ssh_key_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
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
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2023, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

# Prepare the cmd
DATA_SCIENCE_PTPF_SSH_REPO = (
    "git@gitlab.com:gitlab-data/data-science-projects/propensity-to-purchase.git"
)
DATA_SCIENCE_PTPF_HTTP_REPO = "https://gitlab_analytics:$GITLAB_ANALYTICS_PRIVATE_TOKEN@gitlab.com/gitlab-data/data-science-projects/propensity-to-purchase.git"

clone_data_science_ptpf_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{DATA_SCIENCE_PTPF_HTTP_REPO}";
        else
        export REPO="{DATA_SCIENCE_PTPF_SSH_REPO}";
    fi &&
    echo "git clone -b main --single-branch --filter=blob:none --no-checkout --depth 1 $REPO" &&
    git clone -b main --single-branch --filter=blob:none --no-checkout --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd propensity-to-purchase &&
    git sparse-checkout init &&
    git sparse-checkout set /prod &&
    git checkout $GIT_COMMIT &&
    pwd &&
    cd .."""

# Create the DAG
# Run every the 2nd day of the month at 6AM
dag = DAG(
    "ds_propensity_to_purchase_free",
    default_args=default_args,
    schedule_interval="0 6 2 * *",
)

ptpf_scoring_command = f"""
    {clone_data_science_ptpf_repo_cmd} &&
    cd propensity-to-purchase/prod/saas-free &&
    papermill scoring_code.ipynb -p is_local_development False
"""
KubernetesPodOperator(
    **gitlab_defaults,
    image=ANALYST_IMAGE,
    task_id="propensity-to-purchase-free",
    name="propensity-to-purchase-free",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
    ],
    env_vars=pod_env_vars,
    arguments=[ptpf_scoring_command],
    affinity=get_affinity("data_science"),
    tolerations=get_toleration("data_science"),
    dag=dag,
)
