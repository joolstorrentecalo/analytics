"""
## Info about DAG
This DAG is designed to perform adhoc full or incremental refresh of any required number of models.
Before running this DAG fill the variables for the refresh:
- DBT_MODEL_TO_REFRESH: add your model(s) name, if you have more of them, put a white space among them (ie. MODEL_1 MODEL_2)
- SNOWFLAKE_WAREHOUSE_FOR_REFRESH: Allowed values are ["TRANSFORMING_XS", "TRANSFORMING_S","TRANSFORMING_L","TRANSFORMING_XL","TRANSFORMING_4XL",]
- DBT_TYPE_FOR_REFRESH: The refresh will be full refresh or incremental. Allowed values are [--full-refresh, '']

**All variables should be populated, or job will not start.**
"""
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models.param import Param
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_nosha_cmd,
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
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}
params = {
    "DBT_MODEL_TO_REFRESH": Param(
        "{FILL YOU MODEL(s) NAME}", type="string", min_length=3, max_length=500
    ),
    "SNOWFLAKE_WAREHOUSE_FOR_REFRESH": Param(
        "TRANSFORMING_XS",
        type="string",
        enum=[
            "TRANSFORMING_XS",
            "TRANSFORMING_S",
            "TRANSFORMING_L",
            "TRANSFORMING_XL",
            "TRANSFORMING_4XL",
        ],
    ),
    "DBT_TYPE_FOR_REFRESH": Param(
        "--full-refresh",
        type="string",
        enum=["--full-refresh", ""],
        min_length=0,
        max_length=13,
    ),
}

# Create the DAG
dag = DAG(
    "dbt_manual_refresh",
    default_args=default_args,
    schedule_interval=None,
    description="Ad-hoc dbt full or incremental refresh (depends on parameters)",
    catchup=False,
    params=params,
)
dag.doc_md = __doc__

DBT_MODEL_TO_REFRESH = "{{params.DBT_MODEL_TO_REFRESH}}"
SNOWFLAKE_WAREHOUSE_FOR_REFRESH = "{{params.SNOWFLAKE_WAREHOUSE_FOR_REFRESH}}"
DBT_TYPE_FOR_REFRESH = "{{params.DBT_TYPE_FOR_REFRESH}}"

dbt_manual_refresh_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE={SNOWFLAKE_WAREHOUSE_FOR_REFRESH} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models {DBT_MODEL_TO_REFRESH} {DBT_TYPE_FOR_REFRESH} ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_manual_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-manual-refresh",
    name="dbt-manual-refresh",
    secrets=[
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
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_STATIC_DATABASE,
        MCD_DEFAULT_API_ID,
        MCD_DEFAULT_API_TOKEN,
        SNOWFLAKE_STATIC_DATABASE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_manual_refresh_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)

dbt_manual_refresh
