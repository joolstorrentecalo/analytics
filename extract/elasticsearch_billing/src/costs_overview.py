"""
Extract and load Elasticsearch billing costs overview
"""
import os
from datetime import date, datetime, timedelta
from logging import info

from utility import (
    get_response,
    upload_to_snowflake,
    prep_dataframe,
    get_extraction_start_date_end_date_backfill,
    get_extraction_start_date_end_date_recon,
    get_response_and_upload,
)

config_dict = os.environ.copy()

org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]
table_name = "costs_overview"


def get_costs_overview():
    """
    Get costs overview from Elastic Cloud API from start of current month till present date
    """

    info("Getting costs overview")
    date_today = datetime.utcnow().date()
    # if todays date is not one in date_today, then replace day with 1 and month part  to previous month

    if date_today.day == 1:
        extraction_start_date = date_today - timedelta(days=1)
        extraction_start_date = extraction_start_date.replace(day=1)
    else:
        extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)
    costs_endpoint_url = (
        f"/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
    )
    get_response_and_upload(
        costs_endpoint_url, extraction_start_date, extraction_end_date, table_name
    )


def get_reconciliation_data():
    """
    Get reconciliation data from Elastic Cloud API,
    It is performed on 7 and 14th of every month for the previous month to capture any billing corrections
    """
    date_today = datetime.utcnow().date()
    # if date_today day is 7 or 14 then set extraction_start_date as previous months start date and extraction_end_date as previous months end date
    if date_today.day in [3, 7, 14, 15]:
        info("Performing reconciliation...")
        (
            extraction_start_date,
            extraction_end_date,
        ) = get_extraction_start_date_end_date_recon(date_today)
        costs_endpoint_url = f"/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
        get_response_and_upload(
            costs_endpoint_url, extraction_start_date, extraction_end_date, table_name
        )
    else:
        info("No reconciliation required")


def get_costs_overview_backfill():
    """
    Get costs overview from Elastic Cloud API from start date provided by dag config till previous months_end_date
    """

    info("Getting costs overview")
    (
        extraction_start_date,
        extraction_end_date,
    ) = get_extraction_start_date_end_date_backfill()
    info(
        f"Extraction starting from date {extraction_start_date} till date {extraction_end_date}"
    )
    output_list = []
    # iterate each month over the years in between extraction_start_date and extraction_end_date and call API for each month

    for year in range(extraction_start_date.year, extraction_end_date.year + 1):
        for month in range(1, 13):
            current_month = date(year, month, 1)
            start_date = current_month
            if month != 12:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
            else:
                end_date = date(year + 1, 1, 1) - timedelta(days=1)
            if start_date >= extraction_start_date and end_date <= extraction_end_date:
                info(f"{start_date} till {end_date}")
                costs_endpoint_url = (
                    f"/billing/costs/{org_id}?from={start_date}&to={end_date}"
                )
                data = get_response(costs_endpoint_url)
                row_list = [
                    data,
                    start_date,
                    end_date,
                ]
                output_list.append(row_list)

    # upload this data to snowflake
    columns_list = [
        "payload",
        "extraction_start_date",
        "extraction_end_date",
    ]
    output_df = prep_dataframe(output_list, columns_list)
    upload_to_snowflake(output_df, table_name)


def extract_load_billing_costs_overview():
    """
    Extract and load Elastic Search Billing costs overview from start of current month till present date and perform reconciliation
    """

    info("Starting extraction of Elastic Search Billing Costs Overview")

    # Regular daily load from start of current month date till present date
    get_costs_overview()
    # Capture reconciliation data for previous month
    get_reconciliation_data()
    info("Extraction of Elastic Search Billing Costs Overview completed")
