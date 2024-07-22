"""
Run kantata API export. The API returns all the data for each report
for that particular point in time.

That means that this DAG doesn't support backfilling by chunks, hence catchup=False
"""

import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    KANTATA_OAUTH_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration


def clean_string(string_input: str) -> str:
    """
    Need to clean the Kantata report name to make it a valid Airflow
    task name
    """
    patterns = {
        r"api.*!": "",  # remove `api !` prefix from report_name
        r"[^a-zA-Z0-9_]": "_",  # replace all non-alphanumeric chars
        r"_+": "_",  # replace multiple '_' with one
    }

    cleaned_string = string_input.lower()
    for find, replace in patterns.items():
        cleaned_string = re.sub(find, replace, cleaned_string)

    # remove '_' from any starting or ending positions
    cleaned_string = cleaned_string.strip("_")
    return cleaned_string


KANTATA_REPORTS = [
    "API Download: !NC Project Details Table Including Custom Fields",
    "API Download: !NC Project Budget Details",
    "API Download of ! Rev QBR: Details by Project & User",
    "API Download: ! Forecast: FF + T&M [table] [week]",
    "API Download: ! Remaining to Forecast",
]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}

# Define the DAG
dag = DAG(
    "el_kantata_extract",
    description="Extract data from Kantata API endpoint",
    default_args=default_args,
    # Run shortly before dbt dag which is at 8:45UTC
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 7, 19),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

for kantata_report in KANTATA_REPORTS:
    kantata_extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        f"python kantata/src/kantata.py --reports '{kantata_report}'"
    )
    kantata_task_name = f"kantata_{clean_string(kantata_report)}"

    kantata_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE_3_10,
        task_id=kantata_task_name,
        name=kantata_task_name,
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            KANTATA_OAUTH_TOKEN,
        ],
        env_vars={
            **pod_env_vars,
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
        },
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[kantata_extract_command],
        dag=dag,
    )

    dummy_start >> kantata_task
