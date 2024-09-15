"""
Unit to run data classification tasks
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow_utils import (
    DATA_IMAGE,
    DBT_IMAGE,
    clone_repo_cmd,
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
    SNOWFLAKE_STATIC_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

pod_env_vars = {
    "RUN_DATE": "{{ next_ds }}",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}

DAG_NAME = "data_classification"

DAG_DESCRIPTION = "This DAG run to identify data classification for MNPI and PII data."


secrets = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_STATIC_DATABASE,
]
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2019, 1, 1),
    "retry_delay": timedelta(minutes=1),
    "dagrun_timeout": timedelta(days=3),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}


def get_command(task: str):
    """
    Get the execute command
    """
    if task == "extract_classification":
        operation = "EXTRACT"
    else:
        operation = "CLASSIFY"

    unset = Variable.get("DATA_CLASSIFICATION_UNSET", default_var="FALSE")
    tagging_type = Variable.get(
        "DATA_CLASSIFICATION_TAGGING_TYPE", default_var="INCREMENTAL"
    )
    incremental_load_days = Variable.get("DATA_CLASSIFICATION_DAYS", default_var="90")

    commands = {
        "extract_classification": f"""{dbt_install_deps_nosha_cmd} && dbt --quiet ls --target prod --models tag:mnpi+ --exclude tag:mnpi_exception config.database:$SNOWFLAKE_PREP_DATABASE --output json > mnpi_models.json; ret=$?; python ../../extract/data_classification/extract.py --operation={operation} --date_from=$RUN_DATE --unset={unset} --tagging_type={tagging_type} --incremental_load_days={incremental_load_days}; exit $ret""",
        "execute_classification": f"""{clone_repo_cmd} && cd analytics/extract/data_classification/ && python3 extract.py --operation={operation} --date_from=$RUN_DATE --unset={unset} --tagging_type={tagging_type} --incremental_load_days={incremental_load_days}""",
    }

    return commands[task]


dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    concurrency=1,
    description=DAG_DESCRIPTION,
    catchup=False,
)

task_id = task_name = "extract_classification"


extract_classification = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=task_id,
    name=task_name,
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[get_command(task=task_id)],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    dag=dag,
)

task_id = task_name = "execute_classification"

execute_classification = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=task_id,
    name=task_name,
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[get_command(task=task_id)],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    dag=dag,
)

extract_classification >> execute_classification
