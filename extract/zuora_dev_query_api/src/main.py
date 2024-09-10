"""
main function
"""

import logging
from os import environ as env
from typing import Dict

import yaml
from api import ZuoraQueriesAPI
from fire import Fire
from gitlabdata.orchestration_utils import dataframe_uploader, query_executor


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file=file_path, mode="r", encoding="utf-8") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def filter_manifest(manifest_dict: Dict, load_only_table: str = None) -> Dict:
    """
    When load_only_table specified reduce manifest to keep only relevant table config
    """
    if manifest_dict["tables"].get(load_only_table, False):
        manifest_dict["tables"] = {
            load_only_table: manifest_dict["tables"][load_only_table]
        }

    return manifest_dict


def fetch_data_query_upload(
    zq, table_spec: str, query_string: str, if_exists_parameter: str = "replace"
):
    """
    This function is responsible for executing the passed query_string in data query download the file and upload it to snowflake using dataframe uploader.
    """
    job_id = zq.request_data_query_data(query_string)
    df = zq.get_data_query_file(job_id)
    dataframe_uploader(
        df,
        zq.snowflake_engine,
        table_spec,
        schema="ZUORA_QUERY_API_SANDBOX",
        if_exists=if_exists_parameter,
    )
    logging.info(f"Processed {table_spec}")


def main(file_path: str, load_only_table: str = None) -> None:
    """
    main function to iterate over file and range of data
    """
    config_dict = env.copy()
    zq = ZuoraQueriesAPI(config_dict)

    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    manifest_dict = filter_manifest(manifest_dict, load_only_table)

    for table_spec in manifest_dict.get("tables", ""):
        logging.info(f"Processing {table_spec}")

        if table_spec == "chargecontractualvalue":
            date_interval_list = zq.date_range()
            logging.info(f"The date list : {date_interval_list}")
            logging.info(" Droping the table to allow full reload")
            drop_result = query_executor(
                zq.snowflake_engine,
                "DROP TABLE IF EXISTS zuora_query_api.chargecontractualvalue;",
            )
            logging.info(f"Table delete status: {drop_result}")
            for start_end_date in date_interval_list:
                logging.info(
                    f"The date range for extraction is between start_date= {start_end_date['start_date']} to end_date= {start_end_date['end_date']}"
                )
                query_string = f"{manifest_dict.get('tables', {}).get(table_spec, {}).get('query')}  WHERE updatedOn > timestamp '{start_end_date['start_date']}' and updatedOn <= timestamp '{start_end_date['end_date']}'"
                logging.info(f"Query string prepared : {query_string}")
                fetch_data_query_upload(zq, table_spec, query_string, "append")
        else:
            query_string = (
                manifest_dict.get("tables", {}).get(table_spec, {}).get("query")
            )
            fetch_data_query_upload(zq, table_spec, query_string)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
