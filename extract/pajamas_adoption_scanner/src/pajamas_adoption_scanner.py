"""
Returns the json file stored in "https://gitlab-org.gitlab.io/frontend/pajamas-adoption-scanner/adoption_by_group.json"

Then, uploads this json to Snowflake raw table
"""

import os
import sys
import time
import json

from logging import info, basicConfig, getLogger, error
from typing import Any, Dict

import requests

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
)

ADOPTION_BY_GROUP_URL = "https://gitlab-org.gitlab.io/frontend/pajamas-adoption-scanner/adoption_by_group.json"
config_dict = os.environ.copy()


def make_request(
    request_type: str,
    url: str,
    current_retry_count: int = 0,
    max_retry_count: int = 3,
    **kwargs,
) -> requests.models.Response:
    """Generic function that handles making GET and POST requests"""
    additional_backoff = 20
    if current_retry_count >= max_retry_count:
        raise requests.exceptions.HTTPError(
            f"Manually raising 429 Client Error: Too many retries when calling the {url}."
        )
    try:
        if request_type == "GET":
            response = requests.get(url, **kwargs)
        elif request_type == "POST":
            response = requests.post(url, **kwargs)
        else:
            raise ValueError("Invalid request type")

        response.raise_for_status()
        return response
    except requests.exceptions.RequestException:
        if response.status_code == 429:
            # if no retry-after exists, wait default time
            retry_after = int(response.headers.get("Retry-After", additional_backoff))
            info(f"\nToo many requests... Sleeping for {retry_after} seconds")
            # add some buffer to sleep
            time.sleep(retry_after + (additional_backoff * current_retry_count))
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                current_retry_count=current_retry_count + 1,
                max_retry_count=max_retry_count,
                **kwargs,
            )
        error(f"request exception for url {url}, see below")
        raise


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
