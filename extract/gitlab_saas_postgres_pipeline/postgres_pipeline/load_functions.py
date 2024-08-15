import datetime
import logging
import os
from typing import Dict, Any, Optional
import pytz

from gitlabdata.orchestration_utils import (
    query_executor,
    append_to_xcom_file,
)
from sqlalchemy.engine.base import Engine

from postgres_utils import (
    get_internal_identifier_keys,
    chunk_and_upload,
    chunk_and_upload_metadata,
    id_query_generator,
    get_min_or_max_id,
    INCREMENTAL_LOAD_TYPE_BY_ID,
)


def get_last_load_time() -> Optional[datetime.datetime]:
    last_load_tstamp = os.environ["LAST_LOADED"]
    logging.info(f"last_load_tstamp: {last_load_tstamp}")

    if last_load_tstamp != "":
        return datetime.datetime.strptime(last_load_tstamp, "%Y-%m-%dT%H:%M:%S%z")
    return None


def get_additional_filtering(table_dict: Dict[Any, Any]) -> str:
    """
    get the additional filtering parameter from the manifest
    and insert internal filtering keys where specified in manifest
    """
    additional_filtering = table_dict.get("additional_filtering", "")

    key_mappings = {
        "INTERNAL_NAMESPACE_IDS": get_internal_identifier_keys(["namespace_id"]),
        "INTERNAL_PROJECT_IDS": get_internal_identifier_keys(["project_id"]),
        "INTERNAL_PROJECT_PATHS": get_internal_identifier_keys(["project_path"]),
        "INTERNAL_NAMESPACE_PATHS": get_internal_identifier_keys(["namespace_path"]),
        "INTERNAL_PATHS": get_internal_identifier_keys(
            ["namespace_path", "project_path"]
        ),
    }

    additional_filtering = additional_filtering.format(**key_mappings)

    return additional_filtering


def load_incremental(
    source_engine: Engine,
    target_engine: Engine,
    source_table_name: str,
    table_dict: Dict[Any, Any],
    table_name: str,
    database_type: str,
) -> bool:
    """
    Load tables incrementally based off of the execution date.
    """

    raw_query = table_dict["import_query"]
    additional_filtering = get_additional_filtering(table_dict)

    env = os.environ.copy()

    """
      If postgres replication is too far behind for gitlab_com, then data will not be replicated in this DAGRun that
      will not be replicated in future DAGruns -- thus forcing the DE team to backfill.
      This block of code raises an Exception whenever replication is far enough behind that data will be missed.
    """
    if table_dict["export_schema"] == "gitlab_com":
        # Just fetch and print the last pg_last_xact_replay_timestamp in present in system database or not.

        last_replication_check_query = "select pg_last_xact_replay_timestamp();"
        replication_timestamp_query = (
            "select last_replica_time from public.last_replication_timestamp"
        )
        pg_replication_timestamp = query_executor(
            source_engine, last_replication_check_query
        )[0][0]

        replication_timestamp_value = query_executor(
            source_engine, replication_timestamp_query
        )[0][0]
        logging.info(
            f"Timestamp value from pg_last_xact_replay_timestamp:{pg_replication_timestamp}"
        )
        logging.info(f"Timestamp from the database is : {replication_timestamp_value}")

        replication_timestamp = pytz.UTC.localize(replication_timestamp_value)

        last_load_time = get_last_load_time()

        hours_looking_back = int(env["HOURS"])
        logging.info(env["EXECUTION_DATE"])
        try:
            execution_date = datetime.datetime.strptime(
                env["EXECUTION_DATE"], "%Y-%m-%dT%H:%M:%S%z"
            )
        except ValueError:
            execution_date = datetime.datetime.strptime(
                env["EXECUTION_DATE"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )

        if last_load_time is not None:
            this_run_beginning_timestamp = last_load_time - datetime.timedelta(
                minutes=30
            )  # Allow for 30 minute overlap to ensure late arriving data is not skipped
        else:
            logging.warning(
                "No last load time found, using the earliest of the replication timestamp and execution date."
            )
            this_run_beginning_timestamp = min(
                replication_timestamp, execution_date
            ) - datetime.timedelta(hours=hours_looking_back)

        logging.info(f"Replication is at {replication_timestamp}")

        end_timestamp = min(
            replication_timestamp,
            execution_date,
            this_run_beginning_timestamp + datetime.timedelta(hours=hours_looking_back),
        )

        if this_run_beginning_timestamp > end_timestamp:
            raise Exception(
                "beginning timestamp is after end timestamp -- shouldn't be possible -- erroring"
            )

        append_to_xcom_file(
            {"max_data_available": end_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")}
        )

        env["BEGIN_TIMESTAMP"] = this_run_beginning_timestamp.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        env["END_TIMESTAMP"] = end_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    # If _TEMP exists in the table name, skip it because it needs a full sync
    # If a temp table exists then it needs to finish syncing so don't load incrementally
    if "_TEMP" == table_name[-5:]:
        logging.info(
            f"Table {source_table_name} needs to be backfilled due to schema change, aborting incremental load."
        )
        return False
    query = f"{raw_query.format(**env)} {additional_filtering}"

    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        source_table_name,
        database_type,
    )

    return True


def trusted_data_pgp(
    source_engine: Engine,
    target_engine: Engine,
    source_table_name: str,
    table_dict: Dict[Any, Any],
    table_name: str,
    database_type: str,
) -> bool:
    """
    This function is being used for trusted data framework.
    It is responsible for extracting from postgres and loading data in snowflake.
    """
    raw_query = table_dict["import_query"]
    additional_filtering = ""
    advanced_metadata = False

    logging.info(f"Processing table: {source_table_name}")
    query = f"{raw_query} {additional_filtering}"
    logging.info(query)
    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        source_table_name,
        database_type,
        advanced_metadata,
        False,
    )

    return True


def load_scd(
    source_engine: Engine,
    target_engine: Engine,
    source_table_name: str,
    table_dict: Dict[Any, Any],
    table_name: str,
    database_type: str,
) -> bool:
    """
    Load tables that are slow-changing dimensions.
    """

    # If the schema has changed for the SCD table, treat it like a backfill
    if "_TEMP" == table_name[-5:] or target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(
            f"Table {source_table_name} needs to be recreated to due to schema change. Recreating...."
        )
        backfill = True
    else:
        backfill = False

    raw_query = table_dict["import_query"]
    additional_filtering = get_additional_filtering(table_dict)
    advanced_metadata = table_dict.get("advanced_metadata", False)

    logging.info(f"Processing table: {source_table_name}")
    query = f"{raw_query} {additional_filtering}"

    logging.info(query)
    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        source_table_name,
        database_type,
        advanced_metadata,
        backfill,
    )
    return True


def load_ids(
    database_kwargs: Dict[Any, Any],
    table_dict: Dict[Any, Any],
    initial_load_start_date: datetime.datetime,
    start_pk: int,
    load_by_id_export_type,
    extract_chunksize,
    csv_chunksize,
    database_type: str,
) -> bool:
    """Load a query by chunks of IDs instead of all at once."""

    if (
        "_TEMP" != database_kwargs["target_table"][-5:]
        and load_by_id_export_type != INCREMENTAL_LOAD_TYPE_BY_ID
    ):
        logging.info(
            f"Table {database_kwargs['source_table']} doesn't need a full sync."
        )
        return False

    raw_query = table_dict["import_query"]
    additional_filtering = get_additional_filtering(table_dict)
    primary_key = table_dict["export_table_primary_key"]

    logging.info("Getting max id of Postgres source table...")
    max_pk = get_min_or_max_id(
        primary_key,
        database_kwargs["source_engine"],
        database_kwargs["source_table"],
        "max",
        additional_filtering,
    )

    # Create a generator for queries that are chunked by ID range
    id_queries = id_query_generator(
        primary_key,
        raw_query,
        start_pk,
        max_pk,
        extract_chunksize,
    )

    # Iterate through the generated queries
    for query in id_queries:
        filtered_query = f"{query} {additional_filtering}"
        logging.info(f"\nfiltered_query: {filtered_query}")
        # if no original load_start, need to preserve it for subsequent calls
        initial_load_start_date = chunk_and_upload_metadata(
            filtered_query,
            primary_key,
            database_type,
            max_pk,
            initial_load_start_date,
            database_kwargs,
            csv_chunksize,
            load_by_id_export_type,
        )
    return True


def check_new_tables(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
    database_type: str,
) -> bool:
    """
    Load a set amount of rows for each new table in the manifest. A table is
    considered new if it doesn't already exist in the data warehouse.
    """

    if table_dict["database_type"] == database_type:
        raw_query = table_dict["import_query"].split("WHERE")[0]
        additional_filtering = get_additional_filtering(table_dict)
        advanced_metadata = table_dict.get("advanced_metadata", False)
        primary_key = table_dict["export_table_primary_key"]

        # Figure out if the table exists
        if "_TEMP" != table_name[-5:] and not target_engine.has_table(
            f"{table_name}_TEMP"
        ):
            logging.info(f"Table {table} already exists and won't be tested.")
            return False

        # If the table doesn't exist, load whatever the table has
        query = f"{raw_query} WHERE {primary_key} IS NOT NULL {additional_filtering} LIMIT 100000"
        chunk_and_upload(
            query,
            source_engine,
            target_engine,
            table_name,
            table,
            database_type,
            advanced_metadata,
            backfill=True,
        )

    else:
        logging.info(f"Table {table} is not of type {database_type}.")

    return True
