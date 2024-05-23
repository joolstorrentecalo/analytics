import json
import logging
from os import environ as env
from typing import Tuple

import fire
from google.cloud import bigquery

from big_query_client import BigQueryClient
from yaml import safe_load, YAMLError

config_dict = env.copy()


def get_export(export_name: str, config_path: str) -> Tuple[str, str, dict]:
    """
    retrieve export record attributes as well as gcp project and credentials from gcs_external.yml
    """

    with open(config_path, "r") as yaml_file:
        try:
            stream = safe_load(yaml_file)
        except YAMLError as exc:
            print(exc)

    project = stream["project"]
    gcp_credentials = stream["credentials"]
    export = [
        export
        for export in stream["exports"]
        if (export_name is None or export.get("name") == export_name)
    ][0]

    return project, gcp_credentials, export


def set_bucket_path(branch: str, export: dict) -> None:
    """
    set export parameter for dev environment
    """
    if branch != "master":
        root_dir = export["bucket_path"].index("/", len("gs://"))
        export["bucket_path"] = (
            export["bucket_path"][:root_dir]
            + f"/BRANCH_TEST_{branch}"
            + export["bucket_path"][root_dir:]
        )


def get_partition(export: dict) -> str:
    """
    get date partition parameter
    """
    EXPORT_DATE = config_dict["EXPORT_DATE"]

    if export.get("partition_date_part") is None:
        partition = EXPORT_DATE
    elif export["partition_date_part"] == "d":
        partition = EXPORT_DATE[0:10]
    elif export["partition_date_part"] == "m":
        partition = EXPORT_DATE[0:7]

    return partition


def get_billing_data_query(
    export: dict,
) -> str:
    """
    get sql command, with an appropriate target to run in
    bigquery for daily partition
    """

    EXPORT_DATE = config_dict["EXPORT_DATE"]
    GIT_BRANCH = config_dict["GIT_BRANCH"]

    set_bucket_path(branch=GIT_BRANCH, export=export)

    partition = get_partition(export)

    export_query = export["export_query"].replace("{EXPORT_DATE}", partition)

    return (
        f"EXPORT DATA OPTIONS("
        f"  uri='{export['bucket_path']}/{partition}/*.parquet',"
        f"  format='PARQUET',"
        f"  overwrite=true"
        f"  ) AS"
        f"    {export_query}"
    )


def run_export(
    config_path: str,
    export_name: str,
):
    """
    run export command in bigquery
    """

    project, gcp_credentials, export = get_export(export_name, config_path)

    sql_statement = get_billing_data_query(export)

    logging.info(sql_statement)
    logging.info(f"authenticating with {gcp_credentials}")

    credentials = json.loads(config_dict[gcp_credentials], strict=False)
    bq = BigQueryClient(credentials)
    bq.get_result_from_sql(
        sql_statement,
        project=project,
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )


if __name__ == "__main__":
    logging.basicConfig(level=20)
    fire.Fire(run_export)
    logging.info("Complete.")
