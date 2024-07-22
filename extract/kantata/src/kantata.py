"""
Runs the main flow to extract data from Kantataa API
and then upload to Snowflake
"""

import gzip
import io
import sys
from logging import basicConfig, error, getLogger, info
from typing import Optional

import pandas as pd
from endpoints import (
    create_scheduled_insight_report,
    get_insight_reports,
    get_latest_export,
    get_scheduled_insight_reports,
)
from gitlabdata.orchestration_utils import make_request
from kantata_utils import (
    add_csv_file_extension,
    clean_string,
    config_dict,
    convert_timezone,
    process_args,
    upload_kantanta_to_snowflake,
)
from requests import Response


def _retrieve_latest_matching_item(match_str: str, reports: list, match_key: str):
    """
    Base function to retrieve the latest, matching dictionary from a list
    The value of report[match_key] must equal 'match_str'
    """
    max_created_at = "1970-01-01"
    matching_report = {}

    for report_d in reports:
        if (
            report_d.get(match_key, "") == match_str
            and report_d["created_at"] > max_created_at
        ):
            max_created_at = report_d["created_at"]
            matching_report = report_d

    return matching_report


def retrieve_insight_report_external_identifier(
    report_name: str, insight_reports: list
) -> Optional[str]:
    """
    Retrieve the report 'external_repoort_identifier'
    It's possible that there are multiple Insight Reports with
    the same report_name.
    In this case, return the latest report based off 'created_at'
    """
    latest_insight_report = _retrieve_latest_matching_item(
        report_name, insight_reports, "title"
    )
    return latest_insight_report.get("identifier")


def retrieve_scheduled_insight_report(
    report_external_identifier: str, scheduled_insight_reports: list
) -> Optional[dict]:
    """
    Retrieve the scheduled_insight_report json that matches the report_external_identifier
    There can be multiple scheduled reports with same report_external_identifier...
    in this case, return the latest one
    """
    return _retrieve_latest_matching_item(
        report_external_identifier,
        scheduled_insight_reports,
        "external_report_object_identifier",
    )


def has_valid_latest_export(scheduled_insight_report: dict) -> bool:
    """
    Evaluate if the latest report can be exported
    Valid latest export if:
        status=success
        and report was created between data_interval_start/data_interval_end
    Else not valid export
    """
    # return {} when latest_result value is None
    latest_result = scheduled_insight_report.get("latest_result") or {}
    report_title = scheduled_insight_report["title"]
    report_external_identifier = scheduled_insight_report[
        "external_report_object_identifier"
    ]
    if latest_result.get("status") != "success":
        # Generally means just scheduled report, but raise error just in case
        raise ValueError(
            f"Is this a just-scheduled report? Report_title {report_title} with report_external_identifier {report_external_identifier} cannot be exported because the status is {latest_result.get('status')}, full response: {scheduled_insight_report}"
        )
        # return False

    latest_created_at = latest_result.get("created_at")  # in PST tz
    latest_created_at_utc = convert_timezone(latest_created_at)
    if (
        latest_created_at_utc < config_dict["data_interval_start"]
        or latest_created_at_utc >= config_dict["data_interval_end"]
    ):
        # if report created_at isn't between Airflow dates, log that report not processed
        info(
            f"report_title {report_title} with report_external_identifier {report_external_identifier} cannot be exported because the latest_created_at_utc {latest_created_at_utc} is not between the Airflow execution_dates {config_dict['data_interval_start']} - {config_dict['data_interval_end']}"
        )
        return False

    return True


def download_report_from_s3(latest_export: dict) -> Response:
    """
    Get the url of the latest export (some MavenLink S3 bucket link), and pull it down
    """
    try:
        s3_download_url = latest_export["url"]
    except IndexError as e:
        raise IndexError(
            f"\nLatest_export dict {latest_export} does not have valid download url"
        ) from e
    # no header needed, since it's an AWS S3 url
    return make_request("GET", s3_download_url)


def process_latest_export(
    latest_export: dict, upload_file_name: str
) -> Optional[pd.DataFrame]:
    """
    If the latest export response is successful
    Then process the response by:
        1. Saving the contents of the response as .csv.gz to a file (to be uploaded to Snowflake stage)
        2. Returning a portion of the data as a dataframe
            - used for seeding the table if it doesn't exist
            - useful for previewing the data
    """
    s3_response = download_report_from_s3(latest_export)

    if s3_response.status_code == 200:
        content = s3_response.content
        # Write the content to a file in binary mode
        with open(upload_file_name, "wb") as f:
            f.write(content)

        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            df = pd.read_csv(gz, nrows=5)
            info(f"Sample of exported data, df.head:\n{df.head}")
            return df

    else:
        error(f"The download failed with this message: {s3_response.text}")
        s3_response.raise_for_status()
        return None


def _get_snowflake_table_name(report_name):
    """Returns the snowflake table_name based on the Kantata report name"""
    snowflake_table_name = clean_string(report_name)
    info(f"snowflake_table_name: {snowflake_table_name}")
    return snowflake_table_name


def get_and_process_latest_export(scheduled_insight_report: dict, report_name: str):
    """
    Does the following:
        - Return the latest export response from endpoint
        - From the above response, obtain the S3 download url
        - Request from S3 download url, and save contents into a csv file
        - Upload csv file to Snowflake
    """
    latest_export = get_latest_export(scheduled_insight_report["id"])

    snowflake_table_name = _get_snowflake_table_name(report_name)
    upload_file_name = add_csv_file_extension(snowflake_table_name)
    df = process_latest_export(latest_export, upload_file_name)

    upload_kantanta_to_snowflake(df, snowflake_table_name, upload_file_name)


def main():
    """
    This function runs the main API flow:
        - get all available Insight Reports (scheduled and non-scheduled)
            - get the insight_report external_identifier
        - get scheduled API reports
        - If report is already scheduled, and it has a latest valid report, then extract
        - Else schedule the report
    """
    report_names = process_args()
    info(f"\nreport_names: {report_names}")

    insight_reports = get_insight_reports()
    scheduled_insight_reports = get_scheduled_insight_reports()
    for report_name in report_names:
        info(f"Processing report_name: {report_name}")
        report_external_identifier = retrieve_insight_report_external_identifier(
            report_name, insight_reports
        )
        info(f"With report_external_identifier: {report_external_identifier}")
        # if Kantata report exists
        if report_external_identifier:
            scheduled_insight_report = retrieve_scheduled_insight_report(
                report_external_identifier, scheduled_insight_reports
            )
            # if Kantata report is scheduled
            if scheduled_insight_report:
                if has_valid_latest_export(scheduled_insight_report):
                    get_and_process_latest_export(scheduled_insight_report, report_name)
            else:
                create_scheduled_insight_report(
                    report_name, report_name, report_external_identifier, "daily"
                )
        else:
            raise ValueError(
                f"\nNo report_external_identifier found for {report_name}, are you sure {report_name} exists?"
            )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
