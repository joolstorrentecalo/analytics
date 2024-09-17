"""
Utility file
"""

import os
import sys
from datetime import date, datetime, timedelta
from logging import info, error
import requests

import pandas as pd

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
}
base_url = "https://api.elastic-cloud.com/api/v1"
org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]


def upload_to_snowflake(output_df, table_name):
    """
    This function will upload the dataframe to snowflake
    """
    try:
        info("Uploading records to snowflake...")
        loader_engine = snowflake_engine_factory(config_dict, "LOADER")
        dataframe_uploader(
            output_df,
            loader_engine,
            table_name=table_name,
            schema="elasticsearch_billing",
            if_exists="append",
            add_uploaded_at=True,
        )
        info(f"Uploaded {table_name} to Snowflake")
    except Exception as e:
        error(f"Error uploading to snowflake: {e}")
        sys.exit(1)


def get_response(url):
    """
    This function will get the response from the API
    """
    try:
        url = f"{base_url}{url}"
        response = requests.get(url, headers=HEADERS, timeout=60)
        data = response.json()
        return data
    except Exception as e:
        error(f"API connection failed: {e}")
        sys.exit(1)


def get_list_of_deployments():
    """
    This function will get the list of deployments from the API
    """
    deployments_url = f"/billing/costs/{org_id}/deployments"
    deployments_list = get_response(deployments_url)
    info(deployments_list)
    output_list = []
    for deployments in deployments_list["deployments"]:
        deployment_id = deployments["deployment_id"]
        output_list.append(deployment_id)
    return output_list


def prep_dataframe(data: list, columns: list) -> pd.DataFrame:
    """
    This function will prepare the dataframe for itemised costs by deployment
    and will return the prepared data frame
    Input:
    data = [1,2,3]
    columns = ['c1','c2','c3']

    Return:
    pd.DataFrame
    columns: 'c1','c2','c3'
    head(1): 1, 2, 3
    """
    output_df = pd.DataFrame(
        data,
        columns=columns,
    )
    return output_df


def get_extraction_start_date_end_date_backfill():
    """
    This function will set the extraction start date and end date
    """
    current_date = datetime.utcnow().date()
    start_date = config_dict["extraction_start_date"]
    extraction_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    extraction_end_date = date(current_date.year, current_date.month, 1) - timedelta(
        days=1
    )
    extraction_start_date = extraction_start_date.date()
    return extraction_start_date, extraction_end_date


def get_extraction_start_date_end_date_recon(date_today):
    """
    This function will set the extraction start date and end date
    """
    current_months_first_day = date_today.replace(day=1)
    extraction_end_date = current_months_first_day - timedelta(days=1)
    extraction_start_date = extraction_end_date.replace(day=1)
    return extraction_start_date, extraction_end_date


def get_response_and_upload(
    url, extraction_start_date, extraction_end_date, table_name
):
    """
    This function will get the response from the API and upload to snowflake
    """
    output_list = []
    data = get_response(url)
    row_list = [data, extraction_start_date, extraction_end_date]
    columns_list = [
        "payload",
        "extraction_start_date",
        "extraction_end_date",
    ]
    output_list.append(row_list)
    output_df = prep_dataframe(output_list, columns_list)
    upload_to_snowflake(output_df, table_name)


def get_response_and_upload_costs_by_deployments(
    extraction_start_date, extraction_end_date, table_name
):
    """
    This function will get the response from the API and upload to snowflake
    """
    deployments_list = get_list_of_deployments()
    output_list = []
    for deployments in deployments_list:
        deployment_id = deployments
        info(
            f"Retrieving itemized costs for deployment {deployment_id} from {extraction_start_date} till {extraction_end_date}"
        )
        itemised_costs_by_deployments_url = f"/billing/costs/{org_id}/deployments/{deployment_id}/items?from={extraction_start_date}&to={extraction_end_date}"
        data = get_response(itemised_costs_by_deployments_url)
        # upload this data to snowflake
        row_list = [
            deployment_id,
            data,
            extraction_start_date,
            extraction_end_date,
        ]
        output_list.append(row_list)
    columns_list = [
        "deployment_id",
        "payload",
        "extraction_start_date",
        "extraction_end_date",
    ]
    output_df = prep_dataframe(output_list, columns_list)
    upload_to_snowflake(output_df, table_name)
