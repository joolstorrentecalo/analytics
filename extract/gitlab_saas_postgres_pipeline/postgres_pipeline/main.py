import os
import logging
from typing import Dict

from fire import Fire
from gitlabdata.orchestration_utils import (
    query_executor,
    append_to_xcom_file,
)
from sqlalchemy.engine.base import Engine
from postgres_pipeline_table import PostgresPipelineTable
from postgres_utils import (
    chunk_and_upload,
    get_engines,
    id_query_generator,
    manifest_reader,
)


def filter_manifest(manifest_dict: Dict, load_only_table: str = None) -> None:
    # When load_only_table specified reduce manifest to keep only relevant table config
    if load_only_table and load_only_table in manifest_dict["tables"].keys():
        manifest_dict["tables"] = {
            load_only_table: manifest_dict["tables"][load_only_table]
        }


def main(
    file_path: str,
    load_type: str,
    connection_info_file_name: str,
    database_type: str,
    load_only_table: str = None,
) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    filter_manifest(manifest_dict, load_only_table)

    connection_manifest_dict = manifest_reader(connection_info_file_name)
    postgres_engine, snowflake_engine, metadata_engine = get_engines(
        connection_manifest_dict["connection_info"], database_type
    )

    logging.info(snowflake_engine)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        current_table = PostgresPipelineTable(table_dict)

        # Call the correct function based on the load_type
        loaded = current_table.do_load(
            load_type, postgres_engine, snowflake_engine, metadata_engine, database_type
        )
        if loaded:
            logging.info(f"Finished upload for table: {table}")

        count_query = f"SELECT COUNT(*) FROM {current_table.get_target_table_name()}"
        count = 0

        try:
            count = query_executor(snowflake_engine, count_query)[0][0]
        except:
            pass  # likely that the table doesn't exist -- don't want an error here to stop the task

        append_to_xcom_file(
            {current_table.get_target_table_name(): count, "load_ran": loaded}
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
