"""
Functions to call various Kantata API endpoints
"""

from datetime import datetime, time
from logging import error, info
from typing import Optional

import requests
from gitlabdata.orchestration_utils import make_request
from kantata_utils import HEADERS

BASE_ENDPOINT = "https://api.mavenlink.com/api/v1"


def _call_endpoint(
    endpoint,
    endpoint_purpose,
    is_print_response=False,
    **make_request_kwargs,
):
    """
    Base function to call Kantata endpoint
    No mypy type hints here because of open issue: https://github.com/python/mypy/issues/8862
    """
    url = f"{BASE_ENDPOINT}{endpoint}"
    info(f"{make_request_kwargs['request_type']} {url} for {endpoint_purpose}")
    response = make_request(url=url, headers=HEADERS, **make_request_kwargs)
    if is_print_response:
        info(response.text)

    return response.json()


def get_insight_reports(report_title_to_query: Optional[str] = None) -> dict:
    """
    This endpoint returns a list of all Insight Reports
    https://developer.kantata.com/tag/Insights-Reports

    If an optional `report_title_to_query` arg is passed in, will filter only for matching reports
    The optional arg isn't currently being used because in main(),
    it's possible that multiple reports need to be processed
    """
    endpoint = "/insights_reports"
    endpoint_purpose = "obtaining all possible Insight reports"
    query = {"title": report_title_to_query} if report_title_to_query else {}
    make_request_kwargs = {
        "request_type": "GET",
        "params": query,
    }
    return _call_endpoint(endpoint, endpoint_purpose, **make_request_kwargs)


def get_scheduled_insight_reports() -> dict:
    """
    This endpoint returns a list of all Scheduled Insight Reports
    https://developer.kantata.com/tag/Insights-Reports
    """
    endpoint = "/scheduled_jobs/insights_report_exports"
    endpoint_purpose = "obtaining SCHEDULED Insight Reports"
    make_request_kwargs = {"request_type": "GET"}
    return _call_endpoint(endpoint, endpoint_purpose, **make_request_kwargs)


def _calculate_scheduled_report_start_time() -> str:
    """
    The scheduled report start_time should be today's date, at 7:00 UTC (system rounds down to hour)
    That way the extract can run at 8:00 UTC, and dbt at 8:45 UTC
    """
    today = datetime.today().date()
    datetime_7_15_am = datetime.combine(today, time(7, 15))
    datetime_7_15_am_str = datetime_7_15_am.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime_7_15_am_str


def create_scheduled_insight_report(
    title: str,
    description: str,
    report_external_identifier: str,
    cadence: str,
) -> dict:
    """
    This endpoint returns a list of all Scheduled Insight Reports
    https://developer.kantata.com/tag/Insights-Reports
    """

    endpoint = "/scheduled_jobs/insights_report_exports"
    endpoint_purpose = f"creating new scheduled report '{title}'"
    recurrence = {
        "cadence": cadence,
        "start_time": _calculate_scheduled_report_start_time(),
    }
    payload = {
        "insights_report_export": {
            "title": title,
            "description": description,
            "external_report_object_identifier": report_external_identifier,
            "recurrence": recurrence,
        }
    }

    make_request_kwargs = {"request_type": "POST", "json": payload}
    return _call_endpoint(
        endpoint, endpoint_purpose, is_print_response=True, **make_request_kwargs
    )


def get_latest_export(report_id: str) -> dict:
    """
    This endpoint returns the latest export for a particular scheduled report
    """
    try_count, max_try_count = 0, 3
    latest_export_endpoint = f"{BASE_ENDPOINT}/scheduled_jobs/insights_report_exports/{report_id}/results/latest"
    info(
        f"Requesting {latest_export_endpoint} to get the latest export for report_id {report_id}"
    )
    while try_count < max_try_count:
        response = make_request("GET", latest_export_endpoint, headers=HEADERS)

        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error(f"Error: {e}. Response text: {response.text}")
            try_count += 1
            info(
                f"Will now attempt try_count: {try_count}, for a max of {max_try_count} tries"
            )

    raise RuntimeError(
        f"Failed to get latest export after {max_try_count} attempts. Aborting"
    )


# Unused endpoints ######
def get_scheduled_insight_report(report_id: str) -> dict:
    """
    This endpoint returns one insight report based on a report_id
    Not useful in our case, since we don't always know the report_id beforehand
    """
    endpoint = "/scheduled_jobs/insights_report_exports/{report_id}"
    endpoint_purpose = f"getting scheduled report info for report_id {report_id}"
    make_request_kwargs = {"request_type": "GET"}
    return _call_endpoint(
        endpoint,
        endpoint_purpose,
        is_print_response=True,
        **make_request_kwargs,
    )
