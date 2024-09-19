"""
Helpers to the main level_up module
"""

import datetime
import json
import logging
import os
from typing import Any, Dict

import dateutil.parser
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

config_dict = os.environ.copy()


def upload_dict_to_snowflake(
    upload_dict: Dict[Any, Any],
    schema_name: str,
    stage_name: str,
    table_name: str,
    json_dump_filename: str = "to_upload.json",
):
    """
    Upload upload_dict to Snowflake using snowflake_stage_load_copy_remove()
    """
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    with open(json_dump_filename, "w+", encoding="utf8") as upload_file:
        json.dump(upload_dict, upload_file)

    snowflake_stage_load_copy_remove(
        json_dump_filename,
        f"{schema_name}.{stage_name}",
        f"{schema_name}.{table_name}",
        loader_engine,
    )
    loader_engine.dispose()


def iso8601_to_epoch_ts_ms(iso8601_timestamp: str) -> int:
    """
    Converts a string representation of a timestamp in the ISO-8601 format
    to an epoch timestamp in **milliseconds**.

    Args:
    timestamp (str): The string representation of a timestamp in the ISO-8601 format,
    i.e "2023-02-10T16:44:45.084Z"

    Returns:
    int: Epoch timestamp, i.e number of seconds elapsed since 1/1/1970
    """
    date_time = dateutil.parser.isoparse(iso8601_timestamp).replace(
        tzinfo=datetime.timezone.utc
    )
    # 1/1/1970
    dt_epoch_beginning = datetime.datetime.utcfromtimestamp(0).replace(
        tzinfo=datetime.timezone.utc
    )

    delta = date_time - dt_epoch_beginning
    epoch_ts = delta.total_seconds()
    epoch_ts_ms = int(epoch_ts * 1000)
    return epoch_ts_ms


def epoch_ts_ms_to_datetime_str(epoch_ts_ms: int) -> str:
    """
    convert from epoch ts in milliseconds, i.e 1675904400000
    to datetime str, i.e '2023-02-09 01:00:00'
    """
    date_time = datetime.datetime.utcfromtimestamp(epoch_ts_ms / 1000)
    return date_time.strftime("%Y-%m-%d %H:%M:%S")


def is_invalid_ms_timestamp(epoch_start_ms, epoch_end_ms):
    """
    Checks if timestamp in milliseconds > 9/9/2001
    More info here: https://stackoverflow.com/a/23982005
    """
    if len(str(epoch_start_ms)) != 13 or (len(str(epoch_end_ms)) != 13):
        return True
    return False


def postgres_engine_factory(
    password: str, host: str, database: str, port: str, user: str
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """
    # Inject the values to create the engine
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=10000"},
    )
    logging.info(engine)
    return engine


def get_metadata_engine() -> Engine:
    """
    Returns a postgres engine that is connected to the metadata db
    """
    password = os.environ["GITLAB_METADATA_DB_PASS"]
    host = os.environ["GITLAB_METADATA_DB_HOST"]
    database = os.environ["LEVEL_UP_METADATA_DB_NAME"]
    port = os.environ["GITLAB_METADATA_PG_PORT"]
    user = os.environ["GITLAB_METADATA_DB_USER"]

    metadata_engine = postgres_engine_factory(password, host, database, port, user)
    return metadata_engine
