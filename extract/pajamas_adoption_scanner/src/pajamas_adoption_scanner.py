"""
Returns the json file stored in "https://gitlab-org.gitlab.io/frontend/pajamas-adoption-scanner/adoption_by_group.json"

Then, uploads this json to Snowflake raw table
"""

import os
import sys
import json

from logging import info, basicConfig, getLogger, error
from typing import Any, Dict

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
    make_request,
)

ADOPTION_BY_GROUP_URL = "https://gitlab-org.gitlab.io/frontend/pajamas-adoption-scanner/adoption_by_group.json"
config_dict = os.environ.copy()


def log_json_values(results_dict):
    """Returns some info about JSON response"""
    aggregated_at = results_dict["aggregatedAt"]
    groups = results_dict["groups"]
    info(f"Response has `aggregated_at` value of `{aggregated_at}`")
    info(f"Response returned {len(groups)} groups")


def get_adoption_by_group_json():
    """Return json response"""
    response = make_request("GET", ADOPTION_BY_GROUP_URL)
    results_dict = response.json()
    log_json_values(results_dict)
    return results_dict


def upload_results_dict(results_dict: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Uploads the results_dict to Snowflake
    """
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")

    with open("adoption_by_group.json", "w", encoding="utf8") as upload_file:
        json.dump(results_dict, upload_file)

    snowflake_stage_load_copy_remove(
        "adoption_by_group.json",
        "pajamas_adoption_scanner.pajamas_adoption_scanner_load",
        "pajamas_adoption_scanner.adoption_by_group",
        loader_engine,
    )
    loader_engine.dispose()
    return results_dict


def main():
    """main function"""
    results_dict = get_adoption_by_group_json()
    upload_results_dict(results_dict)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
