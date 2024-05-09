import io
import json
import logging
import sys
from os import environ as env
from typing import Dict, Any, List

import pandas as pd
import requests
from sqlalchemy.engine.base import Engine

from api import GitLabAPI
from gitlabdata.orchestration_utils import (
    query_executor,
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

PROJECT_ID_KEY = "project_id"
PROJECT_PATH_KEY = "project_path"


def get_product_project_list() -> List[str]:
    """
    Extracts the part of product CSV and returns the unique project_ids listed in the CSV.
    """
    url = "https://gitlab.com/gitlab-data/analytics/raw/master/transform/snowflake-dbt/data/projects_part_of_product.csv"
    csv_bytes = requests.get(url, timeout=20).content
    df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
    df = df.drop_duplicates(subset=[PROJECT_ID_KEY])
    project_list = df[[PROJECT_ID_KEY, PROJECT_PATH_KEY]].to_dict(orient="records")
    return project_list


def verify_mr_information(
    pulled_mrs: int,
    mr_project_id: int,
    snowflake_engine: Engine,
    mr_start: str,
    mr_end: str,
) -> None:
    """
    Gets number of MRs present from gitlab_db for the same timeframe.
    If that number doesn't match the number passed in, a warning is logged.
    """
    count_query = f"""
        SELECT count(distinct id)
        FROM RAW.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS
        WHERE updated_at BETWEEN '{mr_start}' AND '{mr_end}'
        AND target_project_id = {mr_project_id}
    """
    result_set = query_executor(snowflake_engine, count_query)
    checked_mr_count = result_set[0][0]
    if checked_mr_count != pulled_mrs:
        logging.warn(
            f"Project {mr_project_id} MR counts didn't match: pulled {pulled_mrs}, see {checked_mr_count} in database."
        )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    if len(sys.argv) < 2:
        logging.error("Script ran without specifying extract.")
        sys.exit(1)

    start = config_dict["START"]
    end = config_dict["END"]

    extract_name = sys.argv[1]

    configurations_dict: Dict[str, Any] = {
        "part_of_product": {
            "file_name": "part_of_product_mrs.json",
            "project_list": get_product_project_list(),
            "schema": "engineering_extracts",
            "stage": "part_of_product_merge_request_extracts",
            "mr_attribute_key": "iid",
        },
        "handbook": {
            "file_name": "handbook_mrs.json",
            "project_list": [
                {PROJECT_ID_KEY: 7764, PROJECT_PATH_KEY: "gitlab-com/www-gitlab-com"}
            ],
            "schema": "handbook",
            "stage": "handbook_load",
            "mr_attribute_key": "web_url",
        },
    }

    if extract_name not in configurations_dict:
        logging.error(f"Could not find configuration for extract {extract_name}")
        sys.exit(1)

    configuration = configurations_dict[extract_name]

    file_name: str = configuration["file_name"]  # type: ignore
    schema: str = configuration["schema"]
    stage: str = configuration["stage"]
    # mr_attributes is either a list of mr_iid or mr_web_url
    mr_attribute_key = configuration["mr_attribute_key"]

    api_token = env["GITLAB_COM_API_TOKEN"]
    api_client = GitLabAPI(api_token)

    for project_list_d in configuration["project_list"]:
        project_id = project_list_d[PROJECT_ID_KEY]
        project_path = project_list_d[PROJECT_PATH_KEY]
        logging.info(f"Extracting project {project_id}, {project_path}.")

        mr_attributes = api_client.get_attributes_for_mrs_for_project(
            project_id, start, end, mr_attribute_key
        )

        verify_mr_information(
            len(mr_attributes), project_id, snowflake_engine, start, end
        )

        wrote_to_file = False

        with open(file_name, "w", encoding="utf-8") as out_file:
            for mr_attribute in mr_attributes:
                if extract_name == "handbook":
                    # mr_attribute=mr_web_url
                    mr_information = api_client.get_mr_webpage(mr_attribute)
                else:
                    # mr_attribute=mr_iid
                    mr_information = api_client.get_mr_graphsql(
                        project_path, mr_attribute
                    )
                if mr_information:
                    out_file.write(json.dumps(mr_information))
                    wrote_to_file = True

        if wrote_to_file:
            snowflake_stage_load_copy_remove(
                file_name,
                f"raw.{schema}.{stage}",
                f"{schema}.{extract_name}_merge_requests_graphsql",
                snowflake_engine,
            )
