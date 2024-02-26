"""
Info about DAG:
This DAG is responsible for running a six-hourly refresh on models tagged
with the "six_hourly" label from Monday to Saturday.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_STATIC_DATABASE,
)

from kubernetes_helpers import get_affinity, get_toleration

pod_env_vars = {**gitlab_pod_env_vars, **{}}

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
    "dbt_six_hourly",
    description="This DAG is responsible for refreshing models at minute 55 past every 6th hour.",
    default_args=default_args,
    schedule_interval="55 */6 * * 1-6",
    catchup=False,
)


# run sfdc_opportunity models on large warehouse
dbt_six_hourly_models_command = f"""
    {dbt_install_deps_nosha_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
    dbt --no-use-colors build --profiles-dir profile --target prod --selector six_hourly_salesforce_opportunity; ret=$?; exit $ret

"""

dbt_six_hourly_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt_six_hourly_models_command",
    name="dbt-six-hourly-models-run",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_six_hourly_models_command],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

dbt_six_hourly_models_task
