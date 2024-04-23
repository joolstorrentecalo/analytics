""" The extraction and loading module for zuora revenue report"""

import sys
import logging
import io
from os import environ as env
from typing import Dict
import pandas as pd
import numpy as np
from fire import Fire
from yaml import load, FullLoader
from google.cloud import storage
from google.oauth2 import service_account
from sheetload.sheetload_dataframe_utils import translate_column_names
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def get_gcs_storage_client():
    """Do the auth and return a usable gcs bucket object."""

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client


def get_files_for_report(bucket: str, output_file_name: str) -> list:
    """
    This function provide all the file under zuora_revenue_report for defined output_file_name.
    This function returns all the files i.e. header and body of the particular type of report.
    """
    source_bucket = get_gcs_storage_client().list_blobs(
        bucket, prefix="RAW_DB/zuora_revenue_report/staging", delimiter=None
    )
    file_list = [file.name for file in source_bucket]
    list_of_files_per_report = []
    for file in file_list:
        # check for the output_file name present in the file_list in the GCS bucket.
        if output_file_name in file:
            list_of_files_per_report.append(file)

    return list_of_files_per_report


def get_file_name_without_path(file_name: str) -> str:
    """separate file name from the bucket path"""
    return file_name.split("/")[-1]


def get_table_to_load(file_name: str, output_file_name: str) -> tuple:
    """This function provide information to which table to load the data and type of load i.e. it is for header or for body of the report"""

    if get_file_name_without_path(file_name).startswith("header"):
        table_to_load = output_file_name + "_header"
        type_of_load = "header"
    else:
        table_to_load = output_file_name + "_body"
        type_of_load = "body"

    logging.info(f"Table Name to which we load file {file_name}:{table_to_load}")
    return table_to_load, type_of_load


def load_report_header_snow(
    schema: str, file_name: str, table_to_load: str, engine: Dict
) -> None:
    """This function is responsible for the loaded the header meta data for respective report."""
    copy_header_query = f"COPY INTO {schema}.{table_to_load} from (SELECT REGEXP_REPLACE(t.$1,'( ){1,}','_') AS metadata_column_name, \
     REGEXP_REPLACE(CONCAT(t.$2,COALESCE(t.$3,'')),'(=){1,}','') AS metadata_column_value ,GET(SPLIT(METADATA$FILENAME,'/'),4)  AS file_name \
     FROM @ZUORA_REVENUE_STAGING/{file_name} t ) file_format = (type = csv,field_DELIMITER=',') ;"
    results = query_executor(engine, copy_header_query)
    logging.info(results)


def data_frame_enricher(raw_df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """Add file_name to the data frame before persisting also doing some data cleansing."""
    # raw_df["uploaded_at"] = time()
    raw_df.loc[:, "file_name"] = file_name
    raw_df.columns = [
        translate_column_names(str(column_name)) for column_name in raw_df.columns
    ]
    raw_df = raw_df.infer_objects()
    raw_df.replace("", np.nan, inplace=True)
    return raw_df


def load_static_table(bucket: str, file_name: str, table_to_load: str, engine: Dict):
    """load report which are static in nature body of report with proper data cleansing and adding metadata file_name to the report."""
    raw_file_bucket = get_gcs_storage_client().bucket(bucket)
    raw_file_blob = raw_file_bucket.blob(file_name)
    raw_data = raw_file_blob.download_as_bytes()
    raw_df = pd.read_csv(io.BytesIO(raw_data))
    enriched_df = data_frame_enricher(raw_df, get_file_name_without_path(file_name))

    enriched_df.to_sql(
        name=table_to_load, con=engine, index=False, if_exists="append", chunksize=15000
    )
    logging.info(
        f"Successfully loaded {enriched_df.shape[0]} rows into {table_to_load}"
    )


def load_report_body_snow(
    bucket: str,
    schema: str,
    file_name: str,
    table_to_load: str,
    body_load_query: str,
    report_type: str,
    engine: Dict,
) -> None:
    """Check if the report is of type dynamic. If dynamic report then get the query to build the dynamic query from YAML file and persist into snowflake.
    If the report is static then pass it through the data frame and persist it into snowflake.
    """
    if report_type == "dynamic":
        load_query = body_load_query.replace("XX", "$")
        file_name_without_path = get_file_name_without_path(file_name)
        copy_body_query = f"COPY INTO {schema}.{table_to_load} from (SELECT {load_query} ,'{file_name_without_path}' as file_name \
        FROM @ZUORA_REVENUE_STAGING/{file_name} (FILE_FORMAT => {schema}.revenue_report_format ));"
        results = query_executor(engine, copy_body_query)
        logging.info(results)
    else:
        load_static_table(bucket, file_name, table_to_load, engine)


def move_file_to_processed(bucket: str, file_name: str) -> None:
    """This function is responsible for the moving the successfully loaded file to processed folder."""
    source_bucket = get_gcs_storage_client().bucket(bucket)
    destination_bucket = get_gcs_storage_client().bucket(bucket)
    report_ran_date = file_name.split("/")[-2]
    file_name_without_path = file_name.split("/")[-1]
    destination_file_name = f"RAW_DB/zuora_revenue_report/processed/{report_ran_date}/{file_name_without_path}"
    source_blob = source_bucket.blob(file_name)
    try:
        source_bucket.copy_blob(source_blob, destination_bucket, destination_file_name)
    except FileNotFoundError as exc:
        logging.error(f"Source file {file_name} not found.{exc} ")
        sys.exit(1)
    try:
        source_blob.delete()
    except FileNotFoundError:
        logging.error(
            f"{file_name} is not found , throwing this as error to ensure that we are not overwriting the files."
        )
        sys.exit(1)


def zuora_revenue_report_load(
    bucket: str,
    schema: str,
    output_file_name: str,
    body_load_query: str,
    report_type: str,
) -> None:
    """
    This is main function fired to load the Zuora revenue report file based on type of file it is and to which table particular file needs to persist.
    """
    # Set variable for snowflake engine
    engine = snowflake_engine_factory(config_dict or env, "LOADER", schema)
    # Get the list of file for the particular output_file_name it will contain body and header if only one report is present for a particular type.
    list_of_files = get_files_for_report(bucket, output_file_name)
    logging.info(f"List of files to download for : {list_of_files}")
    # Iterate over each file to load into snowflake and move to processed folder.
    for file_name in list_of_files:
        table_to_load, type_of_load = get_table_to_load(file_name, output_file_name)
        if type_of_load == "header":
            load_report_header_snow(schema, file_name, table_to_load, engine)
            move_file_to_processed(bucket, file_name)
        else:
            load_report_body_snow(
                bucket,
                schema,
                file_name,
                table_to_load,
                body_load_query,
                report_type,
                engine,
            )
            move_file_to_processed(bucket, file_name)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire(
        {
            "zuora_report_load": zuora_revenue_report_load,
        }
    )
