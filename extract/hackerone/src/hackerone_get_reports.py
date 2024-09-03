"""
Extract HackerOne reports from the HackerOne API.
"""
import os
import sys
from datetime import datetime, timedelta
from logging import basicConfig, error, getLogger, info
from time import sleep
from typing import Dict, Tuple, Union

import pandas as pd
import requests
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()
HEADERS = {
    "Accept": "application/json",
}
TIMEOUT = 60
BASE_URL = "https://api.hackerone.com/v1/"
HACKERONE_API_USERNAME = config_dict.get("HACKERONE_API_USERNAME")
HACKERONE_API_TOKEN = config_dict.get("HACKERONE_API_TOKEN")
IS_FULL_REFRESH = config_dict.get("IS_FULL_REFRESH")


def get_start_and_end_date() -> Tuple[str, str]:
    """
    This function will get the start and end date
    """
    # set start date as yesterdays date time and end data as todays date(start of the day at 00:00:00hrs) , if a full refresh is required then default date will be set
    print(f"Full refresh is set to {IS_FULL_REFRESH}")
    if IS_FULL_REFRESH:
        start_date = "2020-01-01T00:00:00Z"
        print(f'start date is set to {start_date}')
    else:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        print(f'start date is set to {start_date}')
    end_date = datetime.now().strftime("%Y-%m-%dT00:0:00Z")
    
    return start_date, end_date


def get_reports(start_date: str, end_date: str) -> pd.DataFrame:
    """
    This function will get the reports from hackerone
    """
    info(f"Getting reports from {start_date} to {end_date}")
    # call hackerone reports api and loop through paginated results
    reports_df = pd.DataFrame()
    page = 1
    while True:
        info(f"Getting reports, extracting page {page}")
        params: Dict[str, Union[int, str]] = {
            "filter[program][]": "gitlab",
            "page[number]": page,
            "page[size]": 100,
            "filter[last_activity_at__gt]": start_date,
            "filter[last_activity_at__lt]": end_date,
        }
        response = requests.get(
            f"{BASE_URL}reports",
            headers=HEADERS,
            params=params,
            auth=(HACKERONE_API_USERNAME, HACKERONE_API_TOKEN),
            timeout=TIMEOUT,
        )
        if response.status_code == 200:
            response_json = response.json()
            # loop through each id in the response
            for report in response_json["data"]:
                report_data = {
                    "id": report["id"],
                    "state": report["attributes"]["state"],
                    "created_at": report["attributes"]["created_at"],
                    "bounties": report["relationships"]["bounties"],
                }
                reports_df = pd.concat(
                    [reports_df, pd.DataFrame([report_data])], ignore_index=True
                )

            if "next" not in response_json["links"]:
                break
            page += 1  # move on to the next set of paginated results(cursor based paginated)
        elif (
            response.status_code == 429
        ):  # if we hit rate limit, wait 60 seconds and try again
            error(
                f"Rate limit exceeded: {response.status_code}, waiting for 60 seconds before sending another request"
            )
            sleep(60)
        else:
            error(f"Error getting reports: {response.status_code}")
            sys.exit(1)
    # vulnerability_information contains sensitive information, so we need to nullify it
    info("Nullifying vulnerability_information")
    for report in reports_df["bounties"]:
        for bounty in report["data"]:
            bounty["relationships"]["report"]["data"]["attributes"][
                "vulnerability_information"
            ] = None
    return reports_df


def upload_to_snowflake(output_df: pd.DataFrame) -> None:
    """
    This function will upload the dataframe to snowflake
    """
    try:
        loader_engine = snowflake_engine_factory(config_dict, "LOADER")
        dataframe_uploader(
            output_df,
            loader_engine,
            table_name="reports",
            schema="hackerone",
            if_exists="append",
            add_uploaded_at=True,
        )
        info("Uploaded 'reports' to Snowflake")
    except Exception as e:
        error(f"Error uploading to snowflake: {e}")
        sys.exit(1)


def main() -> None:
    """Main function."""
    # set start date and end date
    start_date, end_date = get_start_and_end_date()
    # get reports from endpoint
    reports_df = pd.DataFrame()
    reports_df = get_reports(start_date, end_date)
    # upload payload to snowflake
    upload_to_snowflake(reports_df)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
