import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    partitions,
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
task_secrets = [
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
]

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_snowplow_full_refresh",
    default_args=default_args,
    schedule_interval=None,
    concurrency=4,
    catchup=False,
)


def generate_dbt_command(vars_dict):
    json_dict = json.dumps(vars_dict)

    dbt_generate_command = f"""
        {dbt_install_deps_nosha_cmd} &&
        export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_4XL" &&
        dbt run --profiles-dir profile --target prod --models +snowplow --full-refresh --vars '{json_dict}' ; ret=$?;
        montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"dbt-snowplow-full-refresh-{vars_dict['year']}-{vars_dict['month']}",
        name=f"dbt-snowplow-full-refresh-{vars_dict['year']}-{vars_dict['month']}",
        secrets=task_secrets,
        env_vars=pod_env_vars,
        arguments=[dbt_generate_command],
        affinity=get_affinity("dbt"),
        tolerations=get_toleration("dbt"),
        dag=dag,
    )


dummy_operator = DummyOperator(task_id="start", dag=dag)

dbt_snowplow_combined_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target prod --models legacy.snowplow.combined ; ret=$?;
        montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

dbt_snowplow_combined = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"dbt-snowplow-combined",
    name=f"dbt-snowplow-combined",
    trigger_rule="all_success",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_snowplow_combined_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

for month in partitions(date.today() - timedelta(days=62), date.today(), "month"):
    dummy_operator >> generate_dbt_command(month) >> dbt_snowplow_combined
