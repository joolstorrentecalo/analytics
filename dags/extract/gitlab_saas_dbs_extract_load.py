""" Gitlab.Com Extract and load DAG"""

import os
from tokenize import String
from datetime import datetime, timedelta
from typing import Dict, Any, Union
import yaml

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
    REPO_BASE_PATH,
)

from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    CUSTOMERS_DB_HOST,
    CUSTOMERS_DB_NAME,
    CUSTOMERS_DB_PASS,
    CUSTOMERS_DB_USER,
    GCP_PROJECT,
    GCP_REGION,
    GCP_SERVICE_CREDS,
    GITLAB_BACKFILL_BUCKET,
    GITLAB_BACKFILL_BUCKET_CELLS,
    GITLAB_COM_DB_HOST,
    GITLAB_COM_DB_NAME,
    GITLAB_COM_DB_PASS,
    GITLAB_COM_DB_USER,
    GITLAB_COM_PG_PORT,
    GITLAB_COM_SCD_PG_PORT,
    GITLAB_COM_CI_DB_NAME,
    GITLAB_COM_CI_DB_HOST,
    GITLAB_COM_CI_DB_PASS,
    GITLAB_COM_CI_DB_PORT,
    GITLAB_COM_CI_DB_USER,
    GITLAB_OPS_DB_USER,
    GITLAB_OPS_DB_PASS,
    GITLAB_OPS_DB_HOST,
    GITLAB_OPS_DB_NAME,
    PG_PORT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_WAREHOUSE_MEDIUM,
    GITLAB_METADATA_DB_NAME,
    GITLAB_METADATA_DB_HOST,
    GITLAB_METADATA_DB_PASS,
    GITLAB_METADATA_PG_PORT,
    GITLAB_METADATA_DB_USER,
    GITLAB_METADATA_SCHEMA,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
standard_secrets = [
    GCP_SERVICE_CREDS,
    PG_PORT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_WAREHOUSE_MEDIUM,
    SNOWFLAKE_LOAD_ROLE,
    GITLAB_BACKFILL_BUCKET,
    GITLAB_BACKFILL_BUCKET_CELLS,
]


# Dictionary containing the configuration values for the various Postgres DBs
config_dict: Dict[Any, Any] = {
    "el_customers_scd_db": {
        "cloudsql_instance_name": None,
        "dag_name": "el_saas_customers_scd",
        "database_type": "customers",
        "env_vars": {"DAYS": "1"},
        "extract_schedule_interval": "45 5 * * *",
        "secrets": [
            CUSTOMERS_DB_USER,
            CUSTOMERS_DB_PASS,
            CUSTOMERS_DB_HOST,
            CUSTOMERS_DB_NAME,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "customers",
        "description": "This DAG does full extract & load of customer database(Postgres) to snowflake",
    },
    "el_gitlab_com": {
        "cloudsql_instance_name": None,
        "dag_name": "el_saas_gitlab_com",
        "database_type": "main",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "30 2,15 */1 * *",
        "incremental_backfill_interval": "30 2,15 * * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load  of gitlab.com database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com database(Postgres) to snowflake",
    },
    "el_gitlab_com_ci": {
        "cloudsql_instance_name": None,
        "dag_name": "el_saas_gitlab_com_ci",
        "database_type": "ci",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "30 2,15 */1 * *",
        "incremental_backfill_interval": "30 2,15 * * *",
        "secrets": [
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load of gitlab.com CI* database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com CI* database(Postgres) to snowflake",
    },
    "el_gitlab_com_scd": {
        "cloudsql_instance_name": None,
        "dag_name": "el_saas_gitlab_com_scd",
        "database_type": "main",
        "env_vars": {},
        "extract_schedule_interval": "30 3,15 */1 * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-com-scd",
        "description": "This DAG does Full extract & load of gitlab.com database(Postgres) to snowflake",
    },
    "el_gitlab_com_ci_scd": {
        "cloudsql_instance_name": None,
        "dag_name": "el_saas_gitlab_com_ci_scd",
        "database_type": "ci",
        "env_vars": {},
        "extract_schedule_interval": "00 4,16 */1 * *",
        "secrets": [
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-com-scd",
        "description": "This DAG does Full extract & load of gitlab.com database CI* (Postgres) to snowflake",
    },
    "el_gitlab_ops": {
        "cloudsql_instance_name": "ops-db-restore",
        "dag_name": "el_saas_gitlab_ops",
        "database_type": "ops",
        "env_vars": {"HOURS": "48"},
        "extract_schedule_interval": "0 */6 * * *",
        "incremental_backfill_interval": "0 3 * * *",
        "secrets": [
            GCP_PROJECT,
            GCP_REGION,
            GITLAB_OPS_DB_USER,
            GITLAB_OPS_DB_PASS,
            GITLAB_OPS_DB_HOST,
            GITLAB_OPS_DB_NAME,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-ops",
        "description": "This DAG does Incremental extract & load of Operational database (Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incrmental table extract & load of Operational database(Postgres) to snowflake",
    },
    "el_gitlab_ops_scd": {
        "cloudsql_instance_name": "ops-db-restore",
        "dag_name": "el_saas_gitlab_ops_scd",
        "database_type": "ops",
        "env_vars": {"HOURS": "13"},
        "extract_schedule_interval": "0 2 */1 * *",
        "secrets": [
            GCP_PROJECT,
            GCP_REGION,
            GITLAB_OPS_DB_USER,
            GITLAB_OPS_DB_PASS,
            GITLAB_OPS_DB_HOST,
            GITLAB_OPS_DB_NAME,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 10, 3),
        "task_name": "gitlab-ops",
        "description": "This DAG does Full extract & load of Operational database (Postgres) to snowflake",
    },
    "el_cells_gitlab_com": {
        "cloudsql_instance_name": None,
        "dag_name": "el_cells_gitlab_com",
        "database_type": "cells",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "30 2,15 */1 * *",
        "incremental_backfill_interval": "30 2,15 * * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
            GITLAB_BACKFILL_BUCKET_CELLS,
        ],
        "start_date": datetime(2024, 7, 10),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load of gitlab.com database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com database(Postgres) Cells to snowflake",
    },
    "el_cells_gitlab_com_scd": {
        "cloudsql_instance_name": None,
        "dag_name": "el_cells_gitlab_com_scd",
        "database_type": "cells",
        "env_vars": {},
        "extract_schedule_interval": "00 4,16 */1 * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
            GITLAB_BACKFILL_BUCKET_CELLS,
        ],
        "start_date": datetime(2024, 7, 10),
        "task_name": "gitlab-com-scd",
        "description": "This DAG does Full extract & load of gitlab.com database Cells (Postgres) to snowflake",
    },
}


def get_task_pool(task_name) -> str:
    """Return airflow pool name"""
    return f"{task_name}-pool"


def is_deletes_exempt(table_dict):
    """Determine if table is exempt from the deletes process"""
    return table_dict.get("deletes_exempt", False)


def is_incremental(table_dict):
    """Determine if the extraction is incremental or full extract i.e. SCD"""
    raw_query = table_dict["import_query"]
    is_valid_query = "{EXECUTION_DATE}" in raw_query or "{BEGIN_TIMESTAMP}" in raw_query
    incremental_type = table_dict.get("incremental_type")
    return is_valid_query or incremental_type


def use_cloudsql_proxy(
    dag_name, operation, instance_name, connection_info_file_name, database_type
):
    """Use cloudsql proxy for connecting to ops Database"""
    return f"""
        {clone_repo_cmd} &&
        cd analytics/orchestration &&
        python ci_helpers.py use_proxy --instance_name {instance_name} --command " \
            python ../extract/gitlab_saas_postgres_pipeline/postgres_pipeline/main.py tap  \
            ../extract/gitlab_saas_postgres_pipeline/manifests/{dag_name}_db_manifest.yaml {operation} ../extract/gitlab_saas_postgres_pipeline/manifests/{connection_info_file_name} {database_type}
        "
    """


def get_last_loaded(dag_name: String) -> Union[None, str]:
    """Pull from xcom value  last loaded timestamp for the table"""
    if dag_name == "el_gitlab_ops":
        return None

    xcom_date = datetime.now() - timedelta(hours=54)
    return (
        "{{{{ task_instance.xcom_pull('{task_id}', include_prior_dates=True)['max_data_available'] | "
        "default('{default_date}', true) }}}}".format(
            task_id=task_identifier + "-pgp-extract",
            default_date=xcom_date.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00",
        )
    )


def generate_cmd(dag_name, operation, cloudsql_instance_name, database_type, TASK_TYPE):
    """Generate the command"""
    if TASK_TYPE == "db-scd":
        if database_type == "customers":
            file_name = "el_saas_customers_scd_db_manifest.yaml"
        else:
            file_name = "el_gitlab_dotcom_scd_db_manifest.yaml"
    else:
        file_name = "el_gitlab_dotcom_db_manifest.yaml"
    connection_info_file_name = "el_saas_connection_info.yaml"
    if cloudsql_instance_name is None:
        return f"""
            {clone_repo_cmd} &&
            cd analytics/extract/gitlab_saas_postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests/{file_name} {operation} ../manifests/{connection_info_file_name} {database_type}
        """

    return use_cloudsql_proxy(
        dag_name,
        operation,
        cloudsql_instance_name,
        connection_info_file_name,
        database_type,
    )


def extract_manifest(manifest_file_path):
    """Extract postgres pipeline manifest file"""
    with open(manifest_file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


def extract_table_dict_from_manifest(manifest_contents):
    """Extract table from the manifest file for which extraction needs to be done"""
    return manifest_contents["tables"] if manifest_contents.get("tables") else []


def extract_table_dict_based_on_database_type(table_dict, database_type):
    """Extract the list of tables based on the database type"""
    return_dict = {}
    for table_key, table_values in table_dict.items():
        if table_values["database_type"] == database_type or database_type == "cells":
            output_dict = {table_key: table_values}
            return_dict.update(output_dict)
    return return_dict


# Sync DAG
incremental_backfill_dag_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "dagrun_timeout": timedelta(hours=10),
    "trigger_rule": "all_success",
}

scd_dag_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dagrun_timeout": timedelta(hours=10),
    "trigger_rule": "all_success",
}

# Extract DAG
extract_dag_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "dagrun_timeout": timedelta(hours=6),
    "trigger_rule": "all_success",
}


def get_check_replica_snapshot_command(dag_name):
    """
    The get_check_replica_snapshot_command is responsible for preparing the check_replica_snapshot_command, which is used in the dag configuration.
    """
    base_snapshot_command = (
        "python gitlab_saas_postgres_pipeline/postgres_pipeline/check_snapshot.py"
    )
    check_ci_cmd = f"{base_snapshot_command} check_snapshot_ci"
    check_main_cmd = f"{base_snapshot_command} check_snapshot_main_db_incremental"
    if "el_saas_gitlab_com_ci" in dag_name:
        print("Checking CI DAG...")
        check_replica_snapshot_command = (
            f"{clone_and_setup_extraction_cmd} && " f"{check_ci_cmd}"
        )
    elif dag_name == "el_saas_gitlab_com_scd":
        print("Checking gitlab_dotcom_scd DAG...")
        check_replica_snapshot_command = (
            f"{clone_and_setup_extraction_cmd} && " f"{check_main_cmd}"
        )
    elif dag_name == "el_saas_gitlab_com":
        print("Checking gitlab_dotcom_incremental DAG...")
        check_replica_snapshot_command = (
            f"{clone_and_setup_extraction_cmd} && " f"{check_main_cmd}"
        )
    elif dag_name == "el_cells_gitlab_com" or dag_name == "el_cells_gitlab_com_scd":
        print("Checking cells gitlab_dotcom DAG...")
        check_replica_snapshot_command = (
            f"{clone_and_setup_extraction_cmd} && "
            f"{check_ci_cmd} && "
            f"{check_main_cmd}"
        )
    return check_replica_snapshot_command


def get_check_replica_snapshot_task(dag_name, dag_obj):
    """
    This function is responsible for generating the dag configuration for the replica snapshot DAG.
    """
    check_replica_snapshot_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id="check_replica_snapshot",
        name="check_replica_snapshot",
        secrets=[
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_COM_SCD_PG_PORT,
        ],
        env_vars={**gitlab_pod_env_vars, **config["env_vars"]},
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[get_check_replica_snapshot_command(dag_name)],
        retries=2,
        retry_delay=timedelta(seconds=300),
        dag=dag_obj,
    )
    return check_replica_snapshot_task


# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():
    if "gitlab_com" in config["dag_name"]:
        has_replica_snapshot = True
    else:
        has_replica_snapshot = False

    if "scd" not in source_name:
        extract_dag_args["start_date"] = config["start_date"]
        incremental_backfill_dag_args["start_date"] = config["start_date"]

        extract_dag = DAG(
            f"{config['dag_name']}_db_extract",
            default_args=extract_dag_args,
            schedule_interval=config["extract_schedule_interval"],
            description=config["description"],
            catchup=True,
        )
        dummy_operator = DummyOperator(task_id="dummy_operator", dag=extract_dag)

        if has_replica_snapshot:
            check_replica_snapshot = get_check_replica_snapshot_task(
                config["dag_name"], extract_dag
            )
        with extract_dag:
            # Actual PGP extract
            if config["database_type"] == "ops":
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_saas_gitlab_ops_db_manifest.yaml"
            else:
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_gitlab_dotcom_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_dict_unfiltered = extract_table_dict_from_manifest(manifest)
            table_dict = extract_table_dict_based_on_database_type(
                table_dict_unfiltered, config["database_type"]
            )
            incremental_extracts = []
            deletes_extracts = []

            for table in table_dict:
                # tables that aren't incremental won't be processed by the incremental dag
                if not is_incremental(manifest["tables"][table]):
                    continue

                TASK_TYPE = "db-incremental"
                task_identifier = (
                    f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                )

                incremental_cmd = generate_cmd(
                    config["dag_name"],
                    f"--load_type incremental --load_only_table {table}",
                    config["cloudsql_instance_name"],
                    config["database_type"],
                    TASK_TYPE,
                )

                incremental_extract = KubernetesPodOperator(
                    **gitlab_defaults,
                    image=DATA_IMAGE,
                    task_id=f"{task_identifier}-pgp-extract",
                    name=f"{task_identifier}-pgp-extract",
                    pool=get_task_pool(config["task_name"]),
                    secrets=standard_secrets + config["secrets"],
                    env_vars={
                        **gitlab_pod_env_vars,
                        **config["env_vars"],
                        "TASK_INSTANCE": "{{ task_instance_key_str }}",
                        "LAST_LOADED": get_last_loaded(config["dag_name"]),
                    },
                    affinity=get_affinity("extraction"),
                    tolerations=get_toleration("extraction"),
                    arguments=[incremental_cmd],
                    do_xcom_push=True,
                )
                incremental_extracts.append(incremental_extract)

                if not is_deletes_exempt(manifest["tables"][table]):
                    deletes_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type deletes --load_only_table {table}",
                        config["cloudsql_instance_name"],
                        config["database_type"],
                        TASK_TYPE,
                    )

                    deletes_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=f"{task_identifier}-pgp-extract-deletes",
                        name=f"{task_identifier}-pgp-extract-deletes",
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                            "LAST_LOADED": get_last_loaded(config["dag_name"]),
                            "DATE_INTERVAL_END": "{{ data_interval_end.to_iso8601_string() }}",
                        },
                        affinity=get_affinity("extraction_highmem"),
                        tolerations=get_toleration("extraction_highmem"),
                        arguments=[deletes_cmd],
                        do_xcom_push=True,
                    )

                    deletes_extracts.append(deletes_extract)

            incremental_extracts >> dummy_operator >> deletes_extracts
            if has_replica_snapshot:
                check_replica_snapshot >> incremental_extracts

        globals()[f"{config['dag_name']}_db_extract"] = extract_dag

        incremental_backfill_dag = DAG(
            f"{config['dag_name']}_db_incremental_backfill",
            default_args=incremental_backfill_dag_args,
            catchup=False,
            schedule_interval=config["incremental_backfill_interval"],
            concurrency=1,
            description=config["description_incremental"],
        )

        with incremental_backfill_dag:
            if config["database_type"] == "ops":
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_saas_gitlab_ops_db_manifest.yaml"
            else:
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_gitlab_dotcom_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_dict_unfiltered = extract_table_dict_from_manifest(manifest)
            table_dict = extract_table_dict_based_on_database_type(
                table_dict_unfiltered, config["database_type"]
            )
            if has_replica_snapshot:
                check_replica_snapshot_backfill = get_check_replica_snapshot_task(
                    config["dag_name"], incremental_backfill_dag
                )

            for table in table_dict:
                if is_incremental(manifest["tables"][table]):
                    TASK_TYPE = "backfill"

                    task_identifier = (
                        f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                    )

                    sync_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type backfill --load_only_table {table}",
                        config["cloudsql_instance_name"],
                        config["database_type"],
                        TASK_TYPE,
                    )
                    sync_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=task_identifier,
                        name=task_identifier,
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                        },
                        affinity=get_affinity("extraction"),
                        tolerations=get_toleration("extraction"),
                        arguments=[sync_cmd],
                        do_xcom_push=True,
                    )
                if has_replica_snapshot:
                    check_replica_snapshot_backfill >> sync_extract

        globals()[
            f"{config['dag_name']}_db_incremental_backfill"
        ] = incremental_backfill_dag
    else:
        scd_dag_args["start_date"] = config["start_date"]
        sync_dag = DAG(
            f"{config['dag_name']}_db_sync",
            default_args=scd_dag_args,
            schedule_interval=config["extract_schedule_interval"],
            concurrency=6,
            catchup=False,
            description=config["description"],
        )

        with sync_dag:
            # PGP Extract
            if config["database_type"] == "customers":
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_saas_customers_scd_db_manifest.yaml"
            elif config["database_type"] == "ops":
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_saas_gitlab_ops_scd_db_manifest.yaml"
            else:
                file_path = f"{REPO_BASE_PATH}/extract/gitlab_saas_postgres_pipeline/manifests/el_gitlab_dotcom_scd_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_dict_unfiltered = extract_table_dict_from_manifest(manifest)
            table_dict = extract_table_dict_based_on_database_type(
                table_dict_unfiltered, config["database_type"]
            )
            if has_replica_snapshot:
                check_replica_snapshot_scd = get_check_replica_snapshot_task(
                    config["dag_name"], sync_dag
                )
            for table in table_dict:
                if not is_incremental(manifest["tables"][table]):
                    TASK_TYPE = "db-scd"

                    task_identifier = (
                        f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                    )

                    # SCD Task
                    scd_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type scd --load_only_table {table}",
                        config["cloudsql_instance_name"],
                        config["database_type"],
                        TASK_TYPE,
                    )

                    scd_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=task_identifier,
                        name=task_identifier,
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                            "task_id": task_identifier,
                        },
                        arguments=[scd_cmd],
                        affinity=get_affinity("extraction_highmem"),
                        tolerations=get_toleration("extraction_highmem"),
                        do_xcom_push=True,
                    )
                    if has_replica_snapshot:
                        check_replica_snapshot_scd >> scd_extract
        globals()[f"{config['dag_name']}_db_sync"] = sync_dag
