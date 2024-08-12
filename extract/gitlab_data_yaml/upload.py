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
    table_to_upload: str, file_name: str, base_url: str, private_token=None
):
    """
    This function is designed to stream the API content by using Python request library.
    Also, it will be responsible for decoding and generating json file output and upload
    it to external stage of snowflake. Once the file gets loaded it will be deleted from external stage.
    This function can be extended but for now this used for the decoding the encoded content
    """
    info(f"Downloading {file_name} to {file_name}.json file.")

    # Check if there is private token issued for the URL
    if private_token:
        request_url = f"{base_url}{file_name}"
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


def manifest_reader(file_path: str):
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file=file_path, mode="r", encoding="utf8") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def run(file_path: str = "file_specification.yml") -> None:
    """
    Procedure to process files from the manifest file.
    """
    manifest = manifest_reader(file_path=file_path)

    for file, specification in manifest.items():
        info(f"Start processing {file} to Snowflake stage.")

        for table_to_upload, file_name in specification["files"].items():
            streaming = specification.get("streaming", False)

            base_url = specification["URL"]
            if isinstance(base_url, dict):
                base_url = base_url[table_to_upload]

            private_token = specification.get("private_token", None)
            if private_token:
                private_token = env.get(private_token)

            if streaming:
                request_download_decode_upload(
                    table_to_upload=table_to_upload,
                    file_name=file_name,
                    base_url=base_url,
                    private_token=private_token,
                )
            else:
                curl_and_upload(
                    table_to_upload=table_to_upload,
                    file_name=file_name,
                    base_url=base_url,
                    private_token=private_token,
                )

        info(f"End processing {file} to Snowflake stage.")


if __name__ == "__main__":
    run()
