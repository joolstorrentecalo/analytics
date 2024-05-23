import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from yaml import safe_load, YAMLError
from airflow_utils import (
    clone_and_setup_extraction_cmd,
    DATA_IMAGE,
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    REPO_BASE_PATH,
)

from kube_secrets import (
    GCP_MKTG_GOOG_ANALYTICS4_5E6DC7D6_CREDENTIALS,
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SALT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_STATIC_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SALT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_STATIC_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
]

env = os.environ.copy()

GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = gitlab_pod_env_vars

default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 12, 15),
}

dag = DAG(
    "el_google_analytics_four",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    concurrency=1,
    catchup=True,
)

external_table_run_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run-operation stage_external_sources \
        --args "select: source google_analytics_4_bigquery" --profiles-dir profile; ret=$?;
"""
dbt_task_name = "dbt-google-analytics-four-external-table-refresh"
dbt_external_table_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=dbt_task_name,
    trigger_rule="all_done",
    name=dbt_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[external_table_run_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

spec_file = "bigquery/src/google_analytics_four/bigquery_export.yml"
spec_path = f"{REPO_BASE_PATH}/extract/{spec_file}"

with open(
    spec_path,
    "r",
) as yaml_file:
    try:
        stream = safe_load(yaml_file)
    except YAMLError as exc:
        print(exc)

for export in stream["exports"]:
    export_name = export["name"]
    export_date_str = "{{ yesterday_ds_nodash }}"

    billing_extract_command = f"""
    {clone_and_setup_extraction_cmd} &&
    python bigquery/src/bigquery_export.py \
        --config_path={spec_file} \
        --export_name={export_name}
    """

    task_name = export["name"]

    billing_operator = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=export_name,
        name=export_name,
        secrets=[GCP_MKTG_GOOG_ANALYTICS4_5E6DC7D6_CREDENTIALS],
        env_vars={
            **pod_env_vars,
            "EXPORT_DATE": export_date_str,
            "GIT_BRANCH": GIT_BRANCH,
        },
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[billing_extract_command],
        dag=dag,
    )

    billing_operator >> dbt_external_table_run
