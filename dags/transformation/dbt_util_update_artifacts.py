"""
## Info about DAG
This DAG is responsible for doing a model refresh for the tables build by the dbt_atifacts package.  This DAG needs to be run when ever table structure changes are introduced in the package and before any other DAGs are run to prevent errors.
"""

import os
from datetime import datetime, timedelta

from croniter import croniter
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
)

from kubernetes_helpers import get_affinity, get_toleration


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# This value is set based on the commit hash setter task in dbt_snapshot
pull_commit_hash = """export GIT_COMMIT="{{ var.value.dbt_hash }}" """


# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "trigger_rule": TriggerRule.ALL_DONE,
    "dagrun_timeout": timedelta(hours=6),
}

# Define all the  required secret
secrets_list = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
]

# Create the DAG
dag = DAG(
    "dbt_util_update_artifacts",
    description="This DAG is responsible for updating the dbt_artifacts tables",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
dag.doc_md = __doc__

# run non-product models on small warehouse
dbt_non_product_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --select package:dbt_artifacts; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

update_dbt_artifacts = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt_util_update_artifacts",
    name="dbt_util_update_artifacts",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_non_product_models_command],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

update_dbt_artifacts
