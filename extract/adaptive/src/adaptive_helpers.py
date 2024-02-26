import os
import time
import logging
import sys
from datetime import datetime
from logging import info, error
import requests
import pandas as pd
from sqlalchemy.engine.base import Engine
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()
ADAPTIVE_SCHEMA = "adaptive_custom"


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
            f"Manually raising 429 Client Error: \
            Too many retries when calling the {url}."
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


def __query_results_generator(query: str, engine: Engine) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    try:
        query_df_iterator = pd.read_sql(sql=query, con=engine)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
    return query_df_iterator


def read_processed_versions_table() -> pd.DataFrame:
    """Read from processed_versions table to see if
    a version has been processed yet
    """
    table = "processed_versions"
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")

    query = f"""
    select * from {ADAPTIVE_SCHEMA}.{table}
    """
    dataframe = __query_results_generator(query, loader_engine)
    return dataframe


def upload_exported_data(dataframe: pd.DataFrame):
    """Upload an Adaptive export to Snowflake"""
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(
        dataframe,
        loader_engine,
        table_name="reporting",
        schema=ADAPTIVE_SCHEMA,
        add_uploaded_at=True,
    )
    print("\nuploaded exported data to 'reporting' Snowflake table")


def upload_processed_version(version: str):
    """Upload the name of the processed version to Snowflake"""
    data = {"version": [version], "processed_at": datetime.utcnow()}
    dataframe = pd.DataFrame(data)

    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(
        dataframe,
        loader_engine,
        table_name="processed_versions",
        schema=ADAPTIVE_SCHEMA,
        add_uploaded_at=False,
    )
    print(f"\nuploaded version to 'processed_versions' Snowflake table: {version}")


def __wide_to_long(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Convert month_year columns, i.e 07/2023 into rows
    More info here: https://gitlab.com/gitlab-data/analytics/-/issues/16548#note_1414963180
    """
    default_cols = ["Account Name", "Account Code", "Level Name"]
    df_melted = pd.melt(
        dataframe, id_vars=default_cols, var_name="month_year", value_name="Value"
    ).reset_index()

    df_melted[["Month", "Year"]] = (
        df_melted["month_year"].str.split("/", expand=True).astype(int)
    )

    final_cols = default_cols + ["Year", "Month", "Value"]
    df_melted = df_melted[final_cols]
    return df_melted


def edit_dataframe(dataframe: pd.DataFrame, version: str) -> pd.DataFrame:
    """Transform the dataframe slightly to make it easier to process downstream"""
    dataframe = __wide_to_long(dataframe)
    dataframe["Version"] = version

    new_col_mapping = {
        "Account Name": "ACCOUNT_NAME",
        "Account Code": "ACCOUNT_CODE",
        "Level Name": "LEVEL_NAME",
        "Year": "CALENDAR_YEAR",
        "Month": "CALENDAR_MONTH",
        "Value": "VALUE",
        "Version": "VERSION",
    }
    dataframe.rename(columns=new_col_mapping, inplace=True)
    return dataframe
