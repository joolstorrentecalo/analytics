"""
Based on the DAG `execution_date` and `task_schedule`
derives the fiscal_quarter.

The fiscal_quarter is used to request the `net_arr` Clari endpoint.

There are two possible endpoints as described in the README,
one is a current week forecast, and the other is a historical quarter forecast.

Which endpoint to call is determind by which DAG  it is (daily vs quarterly)

The resulting json object is saved to a file and uploaded to Snowflake
"""

import os
import sys
import time
import json

from datetime import datetime
from logging import info, basicConfig, getLogger, error
from typing import Any, Dict
from dateutil import parser as date_parser

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
    make_request,
)

config_dict = os.environ.copy()
HEADERS = {"apikey": config_dict.get("CLARI_API_KEY")}
TIMEOUT = 60
BASE_URL = "https://api.clari.com/v4"


def _calc_fiscal_quarter(date_time: datetime) -> str:
    """Based on datetime object, return it's Gitlab fiscal quarter"""
    # edge-case, fiscal-year doesn't change in Jan, still Q4
    if date_time.month == 1:
        fiscal_year = date_time.year
    else:
        fiscal_year = date_time.year + 1

    if date_time.month in [2, 3, 4]:
        fiscal_quarter = 1
    elif date_time.month in [5, 6, 7]:
        fiscal_quarter = 2
    elif date_time.month in [8, 9, 10]:
        fiscal_quarter = 3
    else:
        fiscal_quarter = 4

    # Format the fiscal year and quarter as a string
    fiscal_year_quarter = f"{fiscal_year}_Q{fiscal_quarter}"
    return fiscal_year_quarter


def _get_previous_fiscal_quarter(date_time: datetime) -> str:
    """
    Based on datetime object, return it's Gitlab previous fiscal quarter o

    This function isn't currently used. Instead, DAG will control the date.
    """
    current_fiscal_quarter = _calc_fiscal_quarter(date_time)
    current_quarter_int = int(current_fiscal_quarter[-1])
    current_year_int = int(current_fiscal_quarter[:4])

    if current_quarter_int == 1:
        return f"{current_year_int-1}_Q4"
    return f"{current_year_int}_Q{current_quarter_int - 1}"


def get_fiscal_quarter() -> str:
    """
    Return the fiscal quarter based on the passed in dag 'execution_date'

    The goal is for daily DAG runs, return the current fiscal quarter
    and for quarterly runs, return the previous fiscal quarter

    That logic though is handled within the daily/quarterly DAG's
    """
    execution_date = date_parser.parse(config_dict["logical_date"])
    task_schedule = config_dict["task_schedule"]

    info(
        f"Calculating quarter based on the following task_schedule \
        and execution_date: {task_schedule} | {execution_date}"
    )

    # if task_schedule == "daily":
    return _calc_fiscal_quarter(execution_date)

    # else quarterly task schedule
    # return _get_previous_fiscal_quarter(execution_date)


def get_forecast(forecast_id: str, fiscal_quarter: str) -> Dict[Any, Any]:
    """
    Make a GET request to /forecast/{forecastId} endpoint
    This endpoint has less options, i.e can't return historical weeks,
    but easier to use.
    Will be used for the Daily DAG run
    """
    params = {"timePeriod": fiscal_quarter}
    forecast_url = f"{BASE_URL}/forecast/{forecast_id}"
    response = make_request(
        "GET", forecast_url, headers=HEADERS, params=params, timeout=TIMEOUT
    )
    info("Successful response from GET forecast API (latest week only)")
    return response.json()


def start_export_report(forecast_id: str, fiscal_quarter: str) -> str:
    """
    Make POST request to start report export for a specific fiscal_quarter
    """
    export_forecast_url = f"{BASE_URL}/export/forecast/{forecast_id}"

    json_body = {"timePeriod": fiscal_quarter, "includeHistorical": True}
    response = make_request(
        "POST", export_forecast_url, headers=HEADERS, json=json_body, timeout=TIMEOUT
    )
    return response.json()["jobId"]


def get_job_status(job_id: str) -> Dict[Any, Any]:
    """Returns the status of the job with the specified ID."""
    job_status_url = f"{BASE_URL}/export/jobs/{job_id}"
    response = make_request("GET", job_status_url, headers=HEADERS, timeout=TIMEOUT)
    info(f'\njobStatus response:\n {response.json()["job"]}')
    return response.json()["job"]


def poll_job_status(
    job_id: str, wait_interval_seconds: int = 30, max_poll_attempts: int = 5
) -> bool:
    """
    Polls the API for the status of the job with the specified ID,
    waiting for the specified interval between polls.

    Will either return True, or raise an exception if the poll fails
    """
    poll_attempts = 0
    while True:
        status = get_job_status(job_id)["status"]
        poll_attempts += 1
        info(f"Poll attempt {poll_attempts} current status: {status}")
        if status == "DONE":
            info(
                f"job_id {job_id} successfully completed, \
                it is ready for export."
            )
            return True

        if status in ["ABORTED", "FAILED", "CANCELLED"]:
            raise Exception(
                f"job_id {job_id} failed to complete \
                with {status} status"
            )

        if poll_attempts >= max_poll_attempts:  # (SCHEDULED, STARTED) status
            raise TimeoutError(
                f"Poll attempts to the job status API for \
                job_id {job_id} have exceeded \
                maximum poll attempts, aborting."
            )
        time.sleep(wait_interval_seconds)


def get_report_results(job_id: str) -> Dict[Any, Any]:
    """Get the report results as a json/dict object"""
    results_url = f"{BASE_URL}/export/jobs/{job_id}/results"
    response = make_request("GET", results_url, headers=HEADERS, timeout=TIMEOUT)
    info("Successfully obtained report data")
    return response.json()


def upload_results_dict(
    results_dict: Dict[Any, Any], forecast_id: str, fiscal_quarter: str
) -> Dict[Any, Any]:
    """
    Uploads the results_dict to Snowflake
    """
    upload_dict = {
        "data": results_dict,
        # update fiscal_quarter formatting to conform with dim table
        "api_fiscal_quarter": fiscal_quarter.replace("_", "-"),
        "dag_schedule": config_dict["task_schedule"],
        "airflow_task_instance_key_str": config_dict["task_instance_key_str"],
        "api_forecast_id": forecast_id,
    }
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")

    with open("clari.json", "w", encoding="utf8") as upload_file:
        json.dump(upload_dict, upload_file)

    snowflake_stage_load_copy_remove(
        "clari.json",
        "clari.clari_load",
        "clari.net_arr",
        loader_engine,
    )
    loader_engine.dispose()
    return upload_dict


def check_valid_quarter(
    original_fiscal_quarter: str, results_dict: Dict[Any, Any]
) -> None:
    """
    Double check that the data returned from the API
    matches the quarter the user is looking for

    This is a good double-check because if the API endpoint does not
    recognize some parameter, it defaults to the current quarter
    which may not be the intention
    """
    api_fiscal_quarter = results_dict["timePeriods"][0]["timePeriodId"]
    if api_fiscal_quarter != original_fiscal_quarter:
        raise ValueError(
            f"The data returned from the API \
        has an api_fiscal_quarter of {api_fiscal_quarter}\n \
        This does not match the original \
        fiscal quarter {original_fiscal_quarter}. \
        Most likely the original quarter has no data. Aborting..."
        )


def main() -> None:
    """Main driver function"""
    forecast_ids = ["net_arr", "net_arr_ps_summary"]
    for forecast_id in forecast_ids:
        fiscal_quarter = get_fiscal_quarter()
        info(
            f"Processing forecast_id {forecast_id} in fiscal_quarter: {fiscal_quarter}"
        )

        # Daily DAG, only return the latest week
        if config_dict["task_schedule"] == "daily":
            results_dict = get_forecast(forecast_id, fiscal_quarter)

        # Quarterly DAG, return multiple weeks, including first few weeks of new quarter
        elif config_dict["task_schedule"] == "quarterly":
            job_id = start_export_report(forecast_id, fiscal_quarter)
            poll_job_status(job_id)
            results_dict = get_report_results(job_id)

        check_valid_quarter(fiscal_quarter, results_dict)
        upload_results_dict(results_dict, forecast_id, fiscal_quarter)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
