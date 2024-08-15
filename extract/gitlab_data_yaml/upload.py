"""
Source code to perform extraction of YAML files from:
1. Gitlab handbook
2. internal handbook
3. compensation calculator
4. cloud connector
"""

import base64
import json
import subprocess
import sys
import traceback
from logging import basicConfig, error, info
from os import environ as env

import requests
import yaml
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

# Configuration
config_dict = env.copy()
basicConfig(stream=sys.stdout, level=20)
snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
gitlab_in_hb_token = env.get("GITLAB_INTERNAL_HANDBOOK_TOKEN")
gitlab_analytics_private_token = config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"]

# URLs
HANDBOOK_URL = "https://gitlab.com/gitlab-com/www-gitlab-com/-/raw/master/data/"
PI_URL = f"{HANDBOOK_URL}performance_indicators/"
PI_INTERNAL_HB_URL = "https://gitlab.com/api/v4/projects/26282493/repository/files/data%2Fperformance_indicators%2F"
COMP_CALC_URL = "https://gitlab.com/api/v4/projects/21924975/repository/files/data%2F"
TEAM_URL = "https://about.gitlab.com/company/team/"
USAGE_PING_METRICS_URL = "https://gitlab.com/api/v4/usage_data/metric_definitions/"
CLOUD_CONNECTOR_URLS = {
    "cloud_connector": "https://gitlab.com/api/v4/projects/2670515/repository/files/config%2F",
    "access_data": "https://gitlab.com/api/v4/projects/278964/repository/files/ee%2Fconfig%2Fcloud_connector%2F",
}

# File settings
# File settings
FILE_MAPPINGS = {
    "handbook": {
        "categories": "categories",
        "stages": "stages",
        "releases": "releases",
    },
    "pi_file": {
        "chief_of_staff_team_pi": "chief_of_staff_team",
        "customer_support_pi": "customer_support_department",
        "development_department_pi": "development_department",
        "engineering_function_pi": "engineering_function",
        "finance_team_pi": "finance_team",
        "infrastructure_department_pi": "infrastructure_department",
        "marketing_pi": "marketing",
        "people_success_pi": "people_success",
        "ux_department_pi": "ux_department",
    },
    "pi_internal_hb_file": {
        "dev_section_pi": "dev_section",
        "core_platform_section_pi": "core_platform_section",
        "ops_section_pi": "ops_section",
        "product_pi": "product",
        "sales_pi": "sales",
        "security_department_pi": "security_division",
    },
    "comp_calc": {
        "location_factors": "location_factors",
        "roles": "job_families",
        "geo_zones": "geo_zones",
    },
    "cloud_connector": {
        "cloud_connector": "cloud_connector",
        "access_data": "access_data",
    },
}


def upload_to_snowflake(file_for_upload: str, table: str) -> None:
    """
    Upload json file to Snowflake
    """
    info(f"....Start uploading to Snowflake, file: {file_for_upload}")
    snowflake_stage_load_copy_remove(
        file=file_for_upload,
        stage="gitlab_data_yaml.gitlab_data_yaml_load",
        table_path=f"gitlab_data_yaml.{table}",
        engine=snowflake_engine,
    )
    info(f"....End uploading to Snowflake, file: {file_for_upload}")


def request_download_decode_upload(
    table_to_upload: str,
    file_name: str,
    base_url: str,
    private_token=None,
    suffix_url=None,
):
    """
    This function is designed to stream the API content by using Python request library.
    Also it will be responsible for decoding and generating json file output and upload
    it to external stage of snowflake. Once the file gets loaded it will be deleted from external stage.
    This function can be extended but for now this used for the decoding the encoded content
    """
    info(f"Downloading {file_name} to {file_name}.json file.")

    # Check if there is private token issued for the URL
    if private_token:
        request_url = f"{base_url}{file_name}{suffix_url}"
        response = requests.request(
            "GET", request_url, headers={"Private-Token": private_token}, timeout=10
        )
    # Load the content in json
    api_response_json = response.json()

    # check if the file is empty or not present.
    record_count = len(api_response_json)

    if record_count > 1:
        # Get the content from response
        file_content = api_response_json.get("content")
        message_bytes = base64.b64decode(file_content)
        output_json_request = yaml.load(message_bytes, Loader=yaml.Loader)

        # write to the Json file
        with open(f"{file_name}.json", "w", encoding="UTF-8") as file_name_json:
            json.dump(output_json_request, file_name_json, indent=4)

        upload_to_snowflake(file_for_upload=f"{file_name}.json", table=table_to_upload)
    else:
        error(
            f"The file for {file_name} is either empty or the location has changed investigate"
        )


def get_json_file_name(input_file: str) -> str:
    """
    Return json file name
    based on input file
    """
    res = ""
    if input_file == "":
        res = "ymltemp"
    elif ".yml" in input_file:
        res = input_file.split(".yml")[0]
    else:
        res = input_file

    return res


def run_subprocess(command: str, file: str) -> None:
    """
    Run subprocess in a separate function

    """
    try:
        process_check = subprocess.run(command, shell=True, check=True)
        process_check.check_returncode()
    except IOError:
        traceback.print_exc()
        error(
            f"The file for {file} is either empty or the location has changed investigate"
        )


def curl_and_upload(
    table_to_upload: str, file_name: str, base_url: str, private_token=None
):
    """
    The function uses curl to download the file and convert the YAML to JSON.
    Then upload the JSON file to external stage and then load it snowflake.
    Post load the files are removed from the external stage
    """
    json_file_name = get_json_file_name(input_file=file_name)

    info(f"Downloading {file_name} to {json_file_name}.json file.")

    if private_token:
        header = f'--header "PRIVATE-TOKEN: {private_token}"'
        command = f"curl {header} '{base_url}{file_name}%2Eyml/raw?ref=main' | yaml2json -o {json_file_name}.json"
    else:
        command = f"curl {base_url}{file_name} | yaml2json -o {json_file_name}.json"

    run_subprocess(command=command, file=file_name)

    info(f"Uploading to {json_file_name}.json to Snowflake stage.")

    upload_to_snowflake(file_for_upload=f"{json_file_name}.json", table=table_to_upload)


if __name__ == "__main__":

    for key, value in FILE_MAPPINGS["handbook"].items():
        curl_and_upload(
            table_to_upload=key, file_name=value + ".yml", base_url=HANDBOOK_URL
        )

    for key, value in FILE_MAPPINGS["pi_file"].items():
        curl_and_upload(table_to_upload=key, file_name=value + ".yml", base_url=PI_URL)

    for key, value in FILE_MAPPINGS["pi_internal_hb_file"].items():
        request_download_decode_upload(
            table_to_upload=key,
            file_name=value,
            base_url=PI_INTERNAL_HB_URL,
            private_token=gitlab_in_hb_token,
            suffix_url="%2Eyml?ref=main",
        )

    for key, value in FILE_MAPPINGS["comp_calc"].items():
        curl_and_upload(
            table_to_upload=key,
            file_name=value,
            base_url=COMP_CALC_URL,
            private_token=gitlab_analytics_private_token,
        )

    for key, value in FILE_MAPPINGS["cloud_connector"].items():
        curl_and_upload(
            table_to_upload=key,
            file_name=value,
            base_url=CLOUD_CONNECTOR_URLS[value],
            private_token=gitlab_analytics_private_token,
        )

    curl_and_upload(table_to_upload="team", file_name="team.yml", base_url=TEAM_URL)
    curl_and_upload(
        table_to_upload="usage_ping_metrics",
        file_name="",
        base_url=USAGE_PING_METRICS_URL,
    )
