import os
from datetime import datetime, timedelta
from yaml import safe_load, YAMLError

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
# Change the DATABASE If the branch if not MASTER
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 7, 1),
    "dagrun_timeout": timedelta(hours=2),
}

config_path = "extract/sheetload/zuora_waterfall/drives.yml"

with open(f"{REPO_BASE_PATH}/{config_path}", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    folders = [folder for folder in stream["folders"]]

# Create the DAG
dag = DAG(
    "waterfall_report_driveload",
    default_args=default_args,
    schedule_interval="15 2/4 3-6 * *",
    concurrency=1,
    catchup=False,
)

driveload_tasks = []

for folder in folders:
    folder_name = folder.get("folder_name")
    table_name = folder.get("table_name")

    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd sheetload/ &&
        python3 sheetload.py drive --drive_file zuora_waterfall/drives.yml --table_name {table_name}
    """

    cleaned_folder_name = folder_name.replace("_", "-")
    driveload_task_id = f"{cleaned_folder_name}-driveload"

    # Task 1
    folder_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=driveload_task_id,
        name=driveload_task_id,
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

    driveload_tasks.append(folder_run)

dbt_run_waterfall_reports_cmd = f"""
    export snowflake_load_database="RAW" &&
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +tag:zuora_waterfall+; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_run_waterfall_reports = KubernetesPodOperator(
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
    arguments=[dbt_run_waterfall_reports_cmd],
    dag=dag,
)

# Order
driveload_tasks >> dbt_run_waterfall_reports
