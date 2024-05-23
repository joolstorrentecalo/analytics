import os
from datetime import datetime, timedelta
from dateutil import rrule

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_PROD_DATABASE": "PROD",
}

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2021, 3, 14),
    "dagrun_timeout": timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    "duo_data_redaction",
    default_args=default_args,
    schedule_interval="0 22 * * *",
    catchup=False,
)

raw_db = gitlab_pod_env_vars["SNOWFLAKE_LOAD_DATABASE"]
prep_db = gitlab_pod_env_vars["SNOWFLAKE_PREP_DATABASE"]
snowplow_tables = [
    {
        "database": raw_db,
        "schema": "snowplow",
        "table": "gitlab_events",
    }
]

days_to_subtract = 90
today_d = datetime.today()
starting_d = today_d - timedelta(days=days_to_subtract)

snowplow_prep_schemas = []

for dt in rrule.rrule(rrule.MONTHLY, dtstart=starting_d, until=today_d):
    snowplow_prep_schemas.append(dt.strftime("SNOWPLOW_%Y_%m"))

for schema in snowplow_prep_schemas:
    snowplow_tables.append(
        {
            "database": prep_db,
            "schema": schema,
            "table": "snowplow_gitlab_events",
        }
    )

for table in snowplow_tables:
    full_name = f"{table['database']}-{table['schema']}-{table['table']}"
    task_identifier = full_name.replace("_", "-").lower()

    run_redaction_command = f"""
      {clone_repo_cmd} &&
      export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
      export SNOWFLAKE_LOAD_WAREHOUSE="TRANSFORMING_XL" &&
      python3 /analytics/orchestration/redact_duo_feedback.py \
        --table={table['table']} \
        --schema={table['schema']} \
        --database={table['database']}
        """

    run_redaction = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE_3_10,
        task_id=task_identifier,
        name=task_identifier,
        secrets=[
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[run_redaction_command],
        dag=dag,
    )
