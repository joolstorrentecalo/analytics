"""
Split backfill of a postgres table into 100 chunks.

From those 100 chunks, based on the max id of the table
evenly distribute the ids to each chunk
"""
import os
from datetime import datetime
from typing import List, Tuple
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)

from kubernetes_helpers import get_affinity, get_toleration

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


def generate_intervals(
    chunks: int, max_id: int, start: int = 1
) -> List[Tuple[int, int]]:
    """
    Generates list of intervals, something like:
    [ (1, 10), (11, 20), (etc, etc) ]
    """
    if chunks > max_id:
        raise ValueError("not enough ids to chunk... aborting")

    intervals = []
    id_range_size = max_id - start + 1
    interval_size = id_range_size // chunks
    remaining = id_range_size % chunks

    for _ in range(chunks):
        end = start + interval_size - 1
        if remaining > 0:
            end += 1
            remaining -= 1
        pair = (start, end)
        intervals.append(pair)
        start = end + 1
    return intervals


def get_interval(intervals: List[Tuple[int, int]], chunk: int) -> Tuple[int, int]:
    interval = intervals[chunk]
    start = interval[0]
    end = interval[1]
    return start, end


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

if GIT_BRANCH in ["master", "main"]:
    target = "prod"
else:
    target = "ci"

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

DBT_MODULE_NAME = "gitlab_dotcom_merge_request_diff_commits_backfill"


# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2023, 8, 28),
}

# Create the DAG
dag = DAG(
    f"dbt_{DBT_MODULE_NAME}",
    default_args=default_args,
    schedule_interval=None,  #
    max_active_runs=2,
    concurrency=2,
    catchup=True,
)
dbt_models_diffs_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target {target} --models internal_merge_request_diffs ; ret=$?;

        montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

DBT_DIFFS_TASK_NAME = "dbt-gitlab_dotcom_merge_request_diffs"
dbt_diffs_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=DBT_DIFFS_TASK_NAME,
    name=DBT_DIFFS_TASK_NAME,
    secrets=dbt_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_models_diffs_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)


CHUNKS = 100
START_ID = 208751592  # merge_request_diff_commits diff_id starts here
MAX_ID = int(
    Variable.get("DBT_GITLAB_DOTCOM_MERGE_REQUEST_DIFF_COMMITS_BACKFILL_MAX_ID")
)

intervals = generate_intervals(CHUNKS, MAX_ID, START_ID)
dbt_commits_tasks = []

for chunk in range(CHUNKS):
    start_id, end_id = get_interval(intervals, chunk)
    dbt_vars = f'{{"backfill_start_id": {start_id}, "backfill_end_id": {end_id}}}'
    dbt_models_commits_cmd = f"""
            {dbt_install_deps_nosha_cmd} &&
            dbt run --profiles-dir profile --target {target} --models internal_merge_request_diff_commits --vars '{dbt_vars}'; ret=$?;

            montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
            python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
            """

    dbt_commits_task_name = f"dbt-{DBT_MODULE_NAME}-{chunk+1:03d}"
    dbt_commits_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=dbt_commits_task_name,
        name=dbt_commits_task_name,
        secrets=dbt_secrets,
        env_vars=pod_env_vars,
        arguments=[dbt_models_commits_cmd],
        affinity=get_affinity("dbt"),
        tolerations=get_toleration("dbt"),
        dag=dag,
    )
    dbt_commits_tasks.append(dbt_commits_task)

dbt_diffs_task >> dbt_commits_tasks
