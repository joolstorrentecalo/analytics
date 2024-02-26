"""Omamori DBT DAG """
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


from airflow_utils import (
    gitlab_defaults,
    slack_failed_task,
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_pod_env_vars,
    run_command_test_exclude,
)
from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
]

DBT_MODULE_NAME = "omamori"

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}

# Define the DAG
dag = DAG(
    "t_omamori_external",
    default_args=default_args,
    schedule_interval="15 * * * *",  # hourly
    start_date=datetime(2023, 5, 6),
    catchup=False,
    max_active_runs=1,
)

# Create/Refresh external tables
external_table_run_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run-operation stage_external_sources \
        --args "select: source {DBT_MODULE_NAME}" --profiles-dir profile; ret=$?;
"""
external_table_task_name = f"dbt-{DBT_MODULE_NAME}-external-table-refresh"
dbt_external_table_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=external_table_task_name,
    name=external_table_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[external_table_run_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

# RUN all omomari source models
model_transform_task_name = f"{DBT_MODULE_NAME}-transform"
# Run de dupe / rename /scd model
model_run_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models sources.{DBT_MODULE_NAME}+ ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
transform_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=model_transform_task_name,
    name=model_transform_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[model_run_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

# TEST all omomari source models
model_test_task_name = f"{DBT_MODULE_NAME}-dbt-tests"
model_test_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models sources.{DBT_MODULE_NAME}+ {run_command_test_exclude} ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
model_test_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=model_test_task_name,
    name=model_test_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[model_test_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

dbt_external_table_run >> transform_task >> model_test_task
