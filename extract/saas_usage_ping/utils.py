"""
Utils unit for Automated Service ping
"""

import datetime
import json
import re
from hashlib import md5
from os import environ as env


import requests

class Utils:
    """
    Utils class for service ping
    """

    ENCODING = "utf8"

    SQL_KEY = "sql"
    REDIS_KEY = "redis"

    META_API_COLUMNS = [
        "recorded_at",
        "version",
        "edition",
        "recording_ce_finished_at",
        "recording_ee_finished_at",
        "uuid",
    ]

    TRANSFORMED_INSTANCE_SQL_QUERIES_FILE = "transformed_instance_queries.json"
    META_DATA_INSTANCE_SQL_QUERIES_FILE = "meta_data_instance_queries.json"
    NAMESPACE_FILE = "usage_ping_namespace_queries.json"

    HAVING_CLAUSE_PATTERN = re.compile(
        "HAVING.*COUNT.*APPROVAL_PROJECT_RULES_USERS.*APPROVALS_REQUIRED",
        re.IGNORECASE,
    )

    METRICS_EXCEPTION_INSTANCE_SQL = (
        "counts.clusters_platforms_eks",
        "counts.clusters_platforms_gke",
        "counts.geo_nodes",
        "counts.issues_created_from_alerts",
        "counts.requirement_test_reports_manual",
        "counts.requirement_test_reports_ci",
        "counts.requirements_with_test_report",
        "counts_weekly.batched_background_migration_count_failed_jobs_metric",
        "usage_activity_by_stage.configure.clusters_platforms_gke",
        "usage_activity_by_stage.configure.clusters_platforms_eks",
        "usage_activity_by_stage_monthly.configure.clusters_platforms_gke",
        "usage_activity_by_stage_monthly.configure.clusters_platforms_eks",
    )

    # Map table which are partioned at the source side but still has same name in snowflake

    RENAMED_TABLE_MAPPING = {
        "p_ci_builds": "ci_builds",
        "p_ci_job_artifacts": "ci_job_artifacts",
    }

    def __init__(self):
        config_dict = env.copy()

        self.headers = {
            "PRIVATE-TOKEN": config_dict.get("GITLAB_ANALYTICS_PRIVATE_TOKEN", None)
        }

    @staticmethod
    def convert_response_to_json(response: requests.Response):
        """
        Convert Response object to json
        """
        return json.loads(response.text)

    def get_response(self, url: str) -> requests.Response:
        """
        get response from the server
        """
        try:
            response = requests.get(url=url, headers=self.headers)
            response.raise_for_status()

            return response
        except requests.ConnectionError as e:
            raise ConnectionError("Error requesting job") from e

    def get_response_as_dict(self, url):
        """
        get prepared response in json format
        """
        response = self.get_response(url=url)

        return self.convert_response_to_json(response=response)

    def keep_meta_data(self, json_data: dict) -> dict:
        """
        Pick up meta-data we want to expose in Snowflake from the original file

        param json_file: json file downloaded from API
        return: dict
        """

        meta_data = {
            meta_api_column: json_data.get(meta_api_column, "")
            for meta_api_column in self.META_API_COLUMNS
        }

        return meta_data

    def save_to_json_file(self, file_name: str, json_data: dict) -> None:
        """
        param file_name: str
        param json_data: dict
        return: None
        """
        with open(file=file_name, mode="w", encoding=self.ENCODING) as wr_file:
            json.dump(json_data, wr_file)

    def load_from_json_file(self, file_name: str):
        """
        Load from json file
        """
        with open(file=file_name, mode="r", encoding=self.ENCODING) as file:
            return json.load(file)

    @staticmethod
    def get_loaded_metadata(keys: list, values: list) -> dict:
        """
        Combined metadata from SQL and Redis and export as one json
        Output:
        {"sql":{"version": "15.1", "edition": "EEU",...},
         "redis": {"version": "15.2", "edition": "CE",...}
         }
        """
        if not keys or not values:
            return {}

        return {keys[i]: values[i] for i in range(len(keys))}

    def get_md5(
        self, input_timestamp: float = datetime.datetime.utcnow().timestamp()
    ) -> str:
        """
        Convert input datetime into md5 hash.
        Result is returned as a string.
        Example:

            Input (datetime): datetime.utcnow().timestamp()
            Output (str): md5 hash

            -----------------------------------------------------------
            current timestamp: 1629986268.131019
            md5 timestamp: 54da37683078de0c1360a8e76d942227
        """
        timestamp_encoded = str(input_timestamp).encode(encoding=self.ENCODING)

        return md5(timestamp_encoded).hexdigest()

    def get_metric_table_name(self, table_name: str) -> str:
        """
        Return table name from dict.
        If table name is not in dict, return the original name
        """
        return self.RENAMED_TABLE_MAPPING.get(table_name, table_name)
