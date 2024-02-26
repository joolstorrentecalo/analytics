"""
The main DAG for checking PVC volume used space 
and alert in case it is over the desired percentage
"""
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import GCP_SERVICE_CREDS
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
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
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "pvc_monitor",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    concurrency=1,
    catchup=False,
    start_date=datetime(2024, 1, 20),
)

container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd pvc_monitor/ &&
        python3 pvc_check.py
    """


# having both xcom flag flavors since we're in an airflow version where one is being deprecated
pvc_monitor = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id="pvc-monitor",
    name="pvc-monitor",
    env_vars=pod_env_vars,
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[container_cmd],
    secrets=[GCP_SERVICE_CREDS],
    do_xcom_push=True,
    dag=dag,
)

pvc_monitor
