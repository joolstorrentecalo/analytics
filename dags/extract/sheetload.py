import os
from datetime import datetime, timedelta
from os import environ as env
from yaml import load, safe_load, YAMLError

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    DBT_IMAGE,
    clone_and_setup_extraction_cmd,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    REPO_BASE_PATH,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
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
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

with open(f"{REPO_BASE_PATH}/extract/sheetload/sheets.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    sheets = [
        "{tab_name}".format(tab_name=tab)
        for sheet in stream["sheets"]
        for tab in sheet["tabs"]
    ]

runs = []

# Create the DAG
dag = DAG(
    "sheetload",
    default_args=default_args,
    schedule_interval="0 1 */1 * *",
    concurrency=1,
    catchup=False,
)

for sheet in sheets:
    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd sheetload/ &&
        python3 sheetload.py sheets --sheet_file sheets.yml --table_name {sheet}
    """

    cleaned_sheet_name = sheet.replace("_", "-")

    # Task 1
    sheetload_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"{cleaned_sheet_name}-sheetload",
        name=f"{cleaned_sheet_name}-sheetload",
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
        arguments=[container_cmd],
        dag=dag,
    )
    runs.append(sheetload_run)

# dbt-sheetload
dbt_sheetload_cmd = f"""
    export snowflake_load_database="RAW" &&
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models sources.sheetload legacy.sheetload; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_sheetload = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-sheetload",
    name="dbt-sheetload",
    secrets=[
        GIT_DATA_TESTS_PRIVATE_KEY,
        GIT_DATA_TESTS_CONFIG,
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_USER,
        MCD_DEFAULT_API_ID,
        MCD_DEFAULT_API_TOKEN,
        SNOWFLAKE_STATIC_DATABASE,
    ],
    env_vars=pod_env_vars,
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    arguments=[dbt_sheetload_cmd],
    dag=dag,
)

# Order
runs >> dbt_sheetload
