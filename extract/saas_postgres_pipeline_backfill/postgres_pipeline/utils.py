import logging
import os
import sys
import yaml
import tempfile
import gcsfs
import pyarrow.parquet as pq

from datetime import datetime, timedelta
from time import time
from typing import Dict, List, Generator, Any, Tuple

from gitlabdata.orchestration_utils import (
    append_to_xcom_file,
    dataframe_uploader,
    dataframe_enricher,
    snowflake_engine_factory,
    query_executor,
)
import pandas as pd
import sqlalchemy
from google.cloud import storage
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket
from google.oauth2 import service_account
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    Date,
    Float,
    DateTime,
    Table,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import CreateTable, DropTable

SCHEMA = "tap_postgres"
BUCKET_NAME = "saas-pipeline-backfills"


def get_gcs_scoped_credentials():
    """Get scoped credential"""
    # create the credentials object
    keyfile = yaml.load(os.environ["GCP_SERVICE_CREDS"], Loader=yaml.FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    scoped_credentials = credentials.with_scopes(scope)
    return scoped_credentials


def get_gcs_bucket() -> Bucket:
    """Do the auth and return a usable gcs bucket object."""
    scoped_credentials = get_gcs_scoped_credentials()
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client.get_bucket(BUCKET_NAME)


def upload_to_gcs(
    advanced_metadata: bool, upload_df: pd.DataFrame, upload_file_name: str
) -> bool:
    """
    Write a dataframe to local storage and then upload it to a GCS bucket.
    """
    bucket = get_gcs_bucket()

    # Write out the parquet and upload it
    enriched_df = dataframe_enricher(advanced_metadata, upload_df)
    os.makedirs(
        os.path.dirname(upload_file_name), exist_ok=True
    )  # need to create director(ies) prior to to_parquet()
    enriched_df.to_parquet(
        upload_file_name,
        compression="gzip",
        index=False,
    )
    logging.info(f"GCS save location: {upload_file_name}")
    blob = bucket.blob(upload_file_name)
    blob.upload_from_filename(upload_file_name)

    return True


def trigger_snowflake_upload(
    engine: Engine, table: str, upload_file_name: str, purge: bool = False
) -> None:
    """Trigger Snowflake to upload a tsv file from GCS."""
    logging.info("Loading from GCS into SnowFlake")

    purge_opt = "purge = true" if purge else ""

    upload_query = f"""
        copy into {table}
        from 'gcs://postgres_pipeline'
        storage_integration = gcs_integration
        pattern = '{upload_file_name}'
        {purge_opt}
        force = TRUE
        file_format = (
            type = csv
            field_delimiter = '\\\\t'
            skip_header = 1
        );
    """
    results = query_executor(engine, upload_query)
    total_rows = 0

    for result in results:
        if result[1] == "LOADED":
            total_rows += result[2]

    log_result = f"Loaded {total_rows} rows from {len(results)} files"
    logging.info(log_result)


def postgres_engine_factory(
    database_connection: Dict[str, str], env: Dict[str, str]
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """

    logging.info(f"\ndatabase_connection: {database_connection}")
    # Set the Vars
    user = env[database_connection["user"]]
    password = env[database_connection["pass"]]
    host = env[database_connection["host"]]
    database = env[database_connection["database"]]
    port = env[database_connection["port"]]

    # Inject the values to create the engine
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    return engine


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def query_results_generator(
    query: str, engine: Engine, chunksize: int = 750_000
) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    try:
        query_df_iterator = pd.read_sql(sql=query, con=engine, chunksize=chunksize)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
    return query_df_iterator


def transform_dataframe_column(column_name: str, pg_type: str) -> List[Column]:
    if pg_type == "timestamp with time zone":
        return Column(column_name, DateTime)
    elif (
        pg_type == "integer"
        or pg_type == "smallint"
        or pg_type == "numeric"
        or pg_type == "bigint"
    ):
        return Column(column_name, Integer)
    elif pg_type == "date":
        return Column(column_name, Date)
    elif pg_type == "boolean":
        return Column(column_name, Boolean)
    elif pg_type == "float" or pg_type == "double precision":
        return Column(column_name, Float)
    else:
        return Column(column_name, String)


def get_postgres_types(table_name: str, source_engine: Engine) -> Dict[str, str]:
    query = f"""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = '{table_name}'
    """
    query_results = query_executor(source_engine, query)
    type_dict = {}
    for result in query_results:
        type_dict[result[0]] = result[1]
    return type_dict


def transform_source_types_to_snowflake_types(
    df: pd.DataFrame, source_table_name: str, source_engine: Engine
) -> List[Column]:
    pg_types = get_postgres_types(source_table_name, source_engine)

    # defaulting to string for any renamed columns or results of functions -- can be cast downstream in dbt source model
    table_columns = [
        transform_dataframe_column(column, pg_types.get(column, "string"))
        for column in df
    ]
    return table_columns


def seed_table(
    advanced_metadata: bool,
    snowflake_types: List[Column],
    target_table_name: str,
    target_engine: Engine,
) -> None:
    """
    Sets the proper data types and column names.
    """
    logging.info(f"Creating table {target_table_name}")
    snowflake_types.append(Column("_uploaded_at", Float))
    if advanced_metadata:
        snowflake_types.append(Column("_task_instance", String))
    table = Table(target_table_name, sqlalchemy.MetaData(), *snowflake_types)
    if target_engine.has_table(target_table_name):
        query_executor(target_engine, DropTable(table))
    query_executor(target_engine, CreateTable(table))
    logging.info(f"{target_table_name} created")


def write_backfill_metadata(
    metadata_engine,
    table_name: str,
    initial_load_start_date: datetime,
    upload_date: datetime,
    upload_file_name: str,
    last_extracted_id: int,
    max_id: int,
    is_backfill_completed: bool,
):
    insert_query = f"""
        INSERT INTO saas_db_metadata.backfill_metadata (
            table_name,
            initial_load_start_date,
            upload_date,
            upload_file_name,
            last_extracted_id,
            max_id,
            is_backfill_completed
        )
        VALUES (
            '{table_name}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_backfill_completed}
        );
    """
    with metadata_engine.connect() as connection:
        connection.execute(insert_query)


def get_upload_file_name(
    table: str,
    initial_load_start_date: datetime,
    upload_date: datetime,
    version: str = None,
    filetype: str = "parquet",
    compression: str = "gzip",
    prefix_template: str = "{table}/{initial_load_prefix}/",
    filename_template: str = "{timestamp}_{table}_{version}.{filetype}.{compression}",
) -> str:
    """Generate a unique and descriptive filename for uploading data to cloud storage.

    Args:
        table (str): The name of the table.
        initial_load_start_date (datetime): The date and time when the initial load started.
        version (str, optional): The version of the data. Defaults to None.
        filetype (str, optional): The file format. Defaults to 'parquet'.
        compression (str, optional): The compression method. Defaults to 'gzip'.
        prefix_template (str, optional): The prefix template for the folder structure.
            Defaults to '{table}/{initial_load_prefix}/'.
        filename_template (str, optional): The filename template.
            Defaults to '{timestamp}_{table}_{version}.{filetype}.{compression}'.

    Returns:
        str: The upload name with the folder structure and filename.
    """
    # Format folder structure
    initial_load_prefix = f"initial_load_start_{initial_load_start_date.isoformat(timespec='milliseconds')}"
    folder_prefix = prefix_template.format(
        table=table, initial_load_prefix=initial_load_prefix
    )

    # Format filename
    timestamp = upload_date.isoformat(timespec="milliseconds")
    if version is None:
        version = ""
    else:
        version = f"_{version}"
    filename = filename_template.format(
        timestamp=timestamp,
        table=table,
        version=version,
        filetype=filetype,
        compression=compression,
    )

    # Combine folder structure and filename
    return os.path.join(folder_prefix, filename).lower()


def chunk_and_upload(
    query: str,
    primary_key: str,
    source_engine: Engine,
    target_engine: Engine,
    metadata_engine: Engine,
    target_table: str,
    source_table: str,
    max_source_id: datetime,
    initial_load_start_date: datetime,
    advanced_metadata: bool = False,
    backfill: bool = False,
) -> None:
    """
    Call the functions that upload the dataframes as TSVs in GCS and then trigger Snowflake
    to load those new files.

    If it is part of a backfill, the first chunk gets sent to the dataframe_uploader
    so that the table can be created automagically with the correct data types.

    Each chunk is uploaded to GCS with a suffix of which chunk number it is.
    All of the chunks are uploaded by using a regex that gets all of the files.
    """

    logging.info(
        f"\ninitial_load_start_date / chunk_upload(): {initial_load_start_date}"
    )
    rows_uploaded = 0

    with tempfile.TemporaryFile() as tmpfile:
        iter_csv = read_sql_tmpfile(query, source_engine, tmpfile)

        for idx, chunk_df in enumerate(iter_csv):
            logging.info(f"\nchunk_df: {chunk_df}")
            """
            if backfill:
                schema_types = transform_source_types_to_snowflake_types(
                    chunk_df, source_table, source_engine
                )
                seed_table(advanced_metadata, schema_types, target_table, target_engine)
                backfill = False
            """

            row_count = chunk_df.shape[0]
            rows_uploaded += row_count
            last_extracted_id = chunk_df[primary_key].max()

            upload_date = datetime.now()
            if initial_load_start_date is None:
                initial_load_start_date = upload_date

            upload_file_name = get_upload_file_name(
                source_table, initial_load_start_date, upload_date
            )

            if row_count > 0:
                upload_to_gcs(advanced_metadata, chunk_df, upload_file_name)
                logging.info(
                    f"Uploaded {row_count} to GCS in {upload_file_name}.{str(idx)}"
                )
                is_backfill_completed = last_extracted_id >= max_source_id
                write_backfill_metadata(
                    metadata_engine,
                    source_table,
                    initial_load_start_date,
                    upload_date,
                    upload_file_name,
                    last_extracted_id,
                    max_source_id,
                    is_backfill_completed,
                )

                logging.info(f"Wrote to backfill metadata db for: {upload_file_name}")
                # TODO: allow us 'mock' a mid backfill
                if last_extracted_id > 2000000 and last_extracted_id < 2700000:
                    return
    """
    if rows_uploaded > 0:
        trigger_snowflake_upload(
            target_engine, target_table, upload_file_name + "[.]\\\\d*", purge=True
        )
        logging.info(f"Uploaded {rows_uploaded} total rows to table {target_table}.")

    target_engine.dispose()
    """
    source_engine.dispose()


def read_sql_tmpfile(query: str, db_engine: Engine, tmp_file: Any) -> pd.DataFrame:
    """
    Uses postGres commands to copy data out of the DB and return a DF iterator
    """
    copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    logging.info(f" running COPY ({query}) TO STDOUT WITH CSV HEADER")
    conn = db_engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert(copy_sql, tmp_file)
    tmp_file.seek(0)
    logging.info("Reading csv")
    df = pd.read_csv(tmp_file, chunksize=750_000, parse_dates=True, low_memory=False)
    logging.info("CSV read")
    return df


def range_generator(
    start: int, stop: int, step: int = 750_000
) -> Generator[Tuple[int, ...], None, None]:
    """
    Yields a list that contains the starting and ending number for a given window.
    """
    while True:
        if start > stop:
            break
        else:
            yield tuple([start, start + step])
        start += step


def is_new_table(metadata_engine: Engine, source_table: str) -> bool:
    """
    Check if backfill table exists in backfill metadata table.
    If the table doesn't exist, then it's a 'new' table
    """
    query = f"SELECT * FROM saas_db_metadata.backfill_metadata WHERE table_name = '{source_table}' LIMIT 1;"
    logging.info(f"\nquery: {query}")
    results = query_executor(metadata_engine, query)
    return len(results) == 0


def query_backfill_status(metadata_engine: Engine, source_table: str) -> bool:
    """
    Check if backfill table exists in backfill metadata table.
    If the table doesn't exist, then it's a 'new' table
    """

    query = (
        "SELECT is_backfill_completed, initial_load_start_date, last_extracted_id, upload_date "
        "FROM saas_db_metadata.backfill_metadata "
        "WHERE upload_date = ("
        "  SELECT MAX(upload_date)"
        " FROM saas_db_metadata.backfill_metadata"
        f" WHERE table_name = '{source_table}');"
    )
    logging.info(f"\nquery backfill status: {query}")
    results = query_executor(metadata_engine, query)
    return results


def is_resume_backfill(self, metadata_engine: Engine):
    """
    Determine if backfill should be resumed.
    First query the backfill database to see if there's a backfill in progress
    If the backfill is in progress, check when the last file was written
    If last file was written within 24 hours, continue from last_extracted_id
    """
    # initialize variables
    is_backfill_needed = False
    start_pk = -1
    initial_load_start_date = None

    results = query_backfill_status(metadata_engine, self.source_table_name)

    # if backfill metadata exists for table
    if results:
        # unpack the results
        (
            is_backfill_completed,
            initial_load_start_date,
            last_extracted_id,
            last_upload_date,
        ) = results[0]
        time_since_last_upload = datetime.now() - last_upload_date

        if not is_backfill_completed:
            is_backfill_needed = True
            # if more than 24 HR since last upload, start backfill over,
            # else proceed with last extracted_id
            if time_since_last_upload > timedelta(hours=24):
                initial_load_start_date = None
                last_extracted_id = 0
                logging.info(
                    f"In middle of backfill, but more than 24 HR has elapsed since last upload: {time_since_last_upload}. Start backfill from beginning."
                )
            start_pk = last_extracted_id + 1
            logging.info(
                f"Resuming backfill with last_extracted_id: {last_extracted_id} and initial_load_start_date: {initial_load_start_date}"
            )

    return is_backfill_needed, start_pk, initial_load_start_date


def get_source_columns(raw_query, source_engine, source_table):
    # Get the columns from the current query
    query_stem = raw_query.lower().split("where")[0]
    source_query = "{0} limit 1"
    source_columns = list(
        pd.read_sql(
            sql=source_query.format(query_stem),
            con=source_engine,
        ).columns
    )
    return source_columns


def get_latest_parquet_file(source_table):
    # first filter for the most recent load_time directory for the source table
    bucket = get_gcs_bucket()

    prefix = f"{source_table}/initial_load_start_"

    blobs = bucket.list_blobs(prefix=prefix)
    parquet_files = []
    for blob in blobs:
        if blob.name.endswith(".parquet.gzip"):
            parquet_files.append(blob.name)

    latest_parquet_file = max(parquet_files)
    return latest_parquet_file


def get_gcs_parquet_schema(parquet_file):
    # create a GCSFileSystem instance with credentials
    scoped_credentials = get_gcs_scoped_credentials()
    fs = gcsfs.GCSFileSystem(project="gitlab-analysis", token=scoped_credentials)

    parquet_file_full_path = f"gs://{BUCKET_NAME}/{parquet_file}"
    logging.info(
        f"\nreading parquet_file_full_path for latest schema: {parquet_file_full_path}"
    )

    with fs.open(parquet_file_full_path) as f:
        arrow_file = pq.ParquetFile(f)
    return arrow_file.schema.names


def get_gcs_columns(source_table):
    latest_file_name = get_latest_parquet_file(source_table)
    gcs_cols = get_gcs_parquet_schema(latest_file_name)
    return gcs_cols


def has_new_columns(source_columns, gcs_columns):
    """
    Checks if source has any new columns compared to gcs

    Note: If GCS has extra columns, this will be ignored as this implies
    that at one point those cols were being brought in from source
    but are no longer needed.
    """
    source_columns, gcs_columns = set(source_columns), set(gcs_columns)
    # does latest gcs NOT include all the source cols?
    return not all(elem in gcs_columns for elem in source_columns)


def schema_addition_check(
    raw_query: str,
    source_engine: Engine,
    source_table: str,
    table_index: str,
) -> bool:
    """
    Query the source table with the manifest query to get the columns, then check
    what columns currently exist in the DW. Return a bool depending on whether
    there has been a change or not.

    If the table does not exist this function will also return True.
    """
    source_columns = get_source_columns(raw_query, source_engine, source_table)
    logging.info(f"\nsource_columns: {source_columns}")

    gcs_columns = get_gcs_columns(source_table)
    logging.info(f"\ngcs_columns: {gcs_columns}")
    return has_new_columns(source_columns, gcs_columns)


def get_min_or_max_id(
    primary_key: str, engine: sqlalchemy.engine.Engine, table: str, min_or_max: str
) -> int:
    """
    Retrieve the minimum or maximum value of the specified primary key column in the specified table.

    Parameters:
    primary_key (str): The name of the primary key column.
    engine (sqlalchemy.engine.Engine): The database engine to use for the query.
    table (str): The name of the table to query.
    min_or_max (str): Either "min" or "max" to indicate whether to retrieve the minimum or maximum ID.

    Returns:
    int: The minimum or maximum ID value.
    """
    logging.info(f"Getting {min_or_max} ID from table: {table}")
    id_query = f"SELECT {min_or_max}({primary_key}) as {primary_key} FROM {table}"
    try:
        id_results = query_results_generator(id_query, engine)
        id_value = next(id_results)[primary_key].tolist()[0]
    except sqlalchemy.exc.ProgrammingError as e:
        logging.exception(e)
        raise
    logging.info(f"{min_or_max} ID: {id_value}")

    if id_value is None:
        logging.info(f"No data found when querying {min_or_max}(id) -- exiting")
        sys.exit(0)
    return id_value


def id_query_generator(
    postgres_engine: Engine,
    primary_key: str,
    raw_query: str,
    snowflake_engine: Engine,
    source_table: str,
    target_table: str,
    min_source_id: int,
    max_source_id: int,
    id_range: int = 750_000,
) -> Generator[str, Any, None]:
    """
    This function generates a list of queries based on the max ID in the target table.

    Gets the diff between the IDs that exist in the DB vs the DW, generates queries for any rows
    with IDs that are missing from the DW.

    i.e. if the table in Snowflake has a max id of 2000, but postgres has a max id of 5000,
    it will return a list of queries that load chunks of IDs until it has the same max id.
    """
    """
    # Get the max ID from the target DB
    logging.info(f"Getting max primary key from target_table: {target_table}")
    max_target_id_query = f"SELECT MAX({primary_key}) as id FROM {target_table}"
    # If the table doesn't exist it will throw an error, ignore it and set a default ID
    if snowflake_engine.has_table(target_table):
        max_target_id_results = query_results_generator(
            max_target_id_query, snowflake_engine
        )
        # Grab the max primary key, or if the table is empty default to 0
        max_target_id = next(max_target_id_results)[primary_key].tolist()[0] or 0
    else:
        max_target_id = 0
    logging.info(f"Target Max ID: {max_target_id}")

    # Get the min ID from the source DB
    logging.info(f"Getting min ID from source_table: {source_table}")
    min_source_id_query = (
        f"SELECT MIN({primary_key}) as {primary_key} FROM {source_table}"
    )
    try:
        min_source_id_results = query_results_generator(
            min_source_id_query, postgres_engine
        )
        min_source_id = next(min_source_id_results)[primary_key].tolist()[0]
    except sqlalchemy.exc.ProgrammingError as e:
        logging.exception(e)
        sys.exit(1)
    logging.info(f"Source Min ID: {min_source_id}")
    """

    # Generate the range pairs based on the max source id and the
    # greatest of either the min_source_id or the max_target_id
    for id_pair in range_generator(min_source_id, max_source_id, step=id_range):
        id_range_query = (
            "".join(raw_query.lower().split("where")[0])
            + f" WHERE {primary_key} BETWEEN {id_pair[0]} AND {id_pair[1]}"
        )
        logging.info(f"ID Range: {id_pair}")
        yield id_range_query


def get_engines(connection_dict: Dict[str, str]) -> Tuple[Engine, Engine, Engine]:
    """
    Generates Snowflake and Postgres engines from env vars and returns them.
    """

    logging.info("Creating database engines...")
    env = os.environ.copy()
    postgres_engine = postgres_engine_factory(
        connection_dict["postgres_source_connection"], env
    )
    metadata_engine = postgres_engine_factory(
        connection_dict["postgres_metadata_connection"], env
    )
    snowflake_engine = snowflake_engine_factory(env, "LOADER", SCHEMA)
    return postgres_engine, metadata_engine, snowflake_engine
