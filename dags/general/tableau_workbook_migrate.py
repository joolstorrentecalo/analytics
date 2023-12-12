import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    TABLEAU_API_SANDBOX_SITE_NAME,
    TABLEAU_API_SANDBOX_TOKEN_NAME,
    TABLEAU_API_SANDBOX_TOKEN_SECRET,
    TABLEAU_API_SANDBOX_URL,
    TABLEAU_API_TOKEN_NAME,
    TABLEAU_API_TOKEN_SECRET,
    TABLEAU_API_URL,
    TABLEAU_API_SITE_NAME,
    TABLEAU_API_PUBLIC_TOKEN_NAME,
    TABLEAU_API_PUBLIC_TOKEN_SECRET,
    TABLEAU_API_PUBLIC_URL,
    TABLEAU_API_PUBLIC_SITE_NAME,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2023, 11, 15),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "tableau_workbook_migrate",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    concurrency=1,
    catchup=False,
)


# tableau Extract
tableau_workbook_migrate_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    TableauConMan provision-settings --yaml_path='./tableau_con_man_config/src/provision_plan.yaml'
"""

# having both xcom flag flavors since we're in an airflow version where one is being deprecated
tableau_provision_users = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id="tableau-provision-users",
    name="tableau-provision-users",
    secrets=[
        TABLEAU_API_SANDBOX_SITE_NAME,
        TABLEAU_API_SANDBOX_TOKEN_NAME,
        TABLEAU_API_SANDBOX_TOKEN_SECRET,
        TABLEAU_API_SANDBOX_URL,
        TABLEAU_API_TOKEN_NAME,
        TABLEAU_API_TOKEN_SECRET,
        TABLEAU_API_URL,
        TABLEAU_API_SITE_NAME,
        TABLEAU_API_PUBLIC_TOKEN_NAME,
        TABLEAU_API_PUBLIC_TOKEN_SECRET,
        TABLEAU_API_PUBLIC_URL,
        TABLEAU_API_PUBLIC_SITE_NAME,
    ],
    env_vars=pod_env_vars,
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[tableau_workbook_migrate_cmd],
    do_xcom_push=True,
    dag=dag,
)

# tableau Extract
tableau_workbook_migrate_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    TableauConMan migrate-content --yaml_path='./tableau_con_man_config/src/public_sync_plan.yaml'
"""

# having both xcom flag flavors since we're in an airflow version where one is being deprecated
tableau_workbook_migrate = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id="tableau-workbook-migrate",
    name="tableau-workbook-migrate",
    secrets=[
        TABLEAU_API_SANDBOX_SITE_NAME,
        TABLEAU_API_SANDBOX_TOKEN_NAME,
        TABLEAU_API_SANDBOX_TOKEN_SECRET,
        TABLEAU_API_SANDBOX_URL,
        TABLEAU_API_TOKEN_NAME,
        TABLEAU_API_TOKEN_SECRET,
        TABLEAU_API_URL,
        TABLEAU_API_SITE_NAME,
        TABLEAU_API_PUBLIC_TOKEN_NAME,
        TABLEAU_API_PUBLIC_TOKEN_SECRET,
        TABLEAU_API_PUBLIC_URL,
        TABLEAU_API_PUBLIC_SITE_NAME,
    ],
    env_vars=pod_env_vars,
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[tableau_workbook_migrate_cmd],
    do_xcom_push=True,
    dag=dag,
)

tableau_provision_users >> tableau_workbook_migrate
