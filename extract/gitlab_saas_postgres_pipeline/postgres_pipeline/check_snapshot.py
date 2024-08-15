"""
    This module is used to check the connectivity of the database and also check the health of the database.
"""

import logging
import sys
import os
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
from gitlabdata.orchestration_utils import query_executor
import fire


def check_snapshot_health(source_engine: Engine) -> None:
    """
    The primary objective of this function is to validate if the snapshot restored is not older than 12 hours.
    If it is older than 12 hours it should fail the task and send notification in slack.
    """
    snapshot_health_query = "SELECT ROUND((EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM PG_LAST_XACT_REPLAY_TIMESTAMP()))/3600);"
    snapshot_age = query_executor(source_engine, snapshot_health_query)[0][0]
    if snapshot_age > 12:
        logging.info(
            "Snapshot restored is older than 12 hours. Please investigate before running downstream"
        )
        sys.exit(1)


def check_snapshot_replica(source_engine: Engine) -> None:
    """
    The check replica snapshot is done by checking the last replay timestamp and current of the postgres database(main or ci).
    If the replay timestamp is not null, the snapshot is successfully restored or else we would need to inspect the data-server-rebuild-ansible pipelines.
    """

    current_date_check_query = "SELECT CURRENT_DATE;"
    pg_current_date_timestamp = query_executor(source_engine, current_date_check_query)[
        0
    ][0]
    # Because sometimes we do not get a pg_last_xact_replay_timestamp, added a step to validate this.
    last_replica_date_check_query = "SELECT pg_last_xact_replay_timestamp();"
    pg_last_xact_replay_timestamp = query_executor(
        source_engine, last_replica_date_check_query
    )[0][0]
    if pg_current_date_timestamp is not None:
        logging.info(f"current_date from Postgres:{pg_current_date_timestamp}")
    else:
        logging.info("No Current date found")
        sys.exit(1)

    if pg_last_xact_replay_timestamp is not None:
        logging.info(
            f"pg_last_xact_replay_timestamp from Postgres:{pg_last_xact_replay_timestamp}"
        )
    else:
        logging.info("No pg_last_xact_replay_timestamp found")
        sys.exit(1)


def check_snapshot_ci() -> None:
    """
    The check snapshot ci method is used to establish connectivity to the database.
    """
    config_dict = os.environ.copy()
    database = config_dict.get("GITLAB_COM_CI_DB_NAME")
    host = config_dict.get("GITLAB_COM_CI_DB_HOST")
    password = config_dict.get("GITLAB_COM_CI_DB_PASS")
    port = config_dict.get("GITLAB_COM_CI_DB_PORT")
    user = config_dict.get("GITLAB_COM_CI_DB_USER")
    logging.info("Creating database engines...")
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    check_snapshot_replica(engine)
    logging.info("Check health of snapshot")
    check_snapshot_health(engine)
    logging.info("Complete")


def check_snapshot_main_db_incremental() -> None:
    """
    The check snapshot main db incremental & scd method is used to establish connectivity to the database.
    """
    config_dict = os.environ.copy()
    database = config_dict.get("GITLAB_COM_DB_NAME")
    host = config_dict.get("GITLAB_COM_DB_HOST")
    password = config_dict.get("GITLAB_COM_DB_PASS")
    port = config_dict.get("GITLAB_COM_PG_PORT")
    user = config_dict.get("GITLAB_COM_DB_USER")
    logging.info("Creating database engines...")
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    check_snapshot_replica(engine)
    logging.info("Check health of snapshot")
    check_snapshot_health(engine)
    logging.info("Complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fire.Fire()
