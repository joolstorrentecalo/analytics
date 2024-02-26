import json
import logging
import sys
from os import environ as env
from typing import List

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError


def update_records_redact_extended_feedback(
    fully_qualified_table_ref: str, redaction_str: str, snowplow_events: List
):
    """
    updates each record from list with a json string, replacing the feedback attribute with the redaction_str
    """

    for key_value, column_value in snowplow_events:

        column_value_json = json.loads(column_value)
        column_value_json["data"][0]["data"]["extra"][
            "extendedFeedback"
        ] = redaction_str
        new_column_value = json.dumps(column_value_json)

        try:
            logging.info(f"redacting from event: event_id = {key_value}")
            update_cmd = f"update {fully_qualified_table_ref} set contexts = $${new_column_value}$$ where event_id ='{key_value}'"
            connection = engine.connect()
            connection.execute(update_cmd)

        except:
            logging.info("Failed to update")
            raise


def get_records_with_extended_feedback(database, schema, table):
    """
    retrieves snowplow events with Duo extended feedback populated
    """

    fully_qualified_table_ref = f'"{database}".{schema}.{table}'
    redaction_str = "<REDACTED>"

    query = f"""
    SELECT event_id, contexts
    FROM {fully_qualified_table_ref}
    WHERE collector_tstamp <= dateadd(days, -60, current_timestamp) 
    AND se_label ='response_feedback'
    AND contexts like '%"extendedFeedback":%'
    AND contexts not like '%{redaction_str}%'
    """

    try:
        logging.info("Getting snowplow events with extended feedback")
        connection = engine.connect()
        duo_feedback_events = connection.execute(query).fetchall()
        record_count = len(duo_feedback_events)

        logging.info(f"updating {record_count} records")
        update_records_redact_extended_feedback(
            fully_qualified_table_ref, redaction_str, duo_feedback_events
        )

    except:
        logging.info("Failed to get snowplow events")
        raise
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    redaction_str = "<REDACTED>"
    Fire(get_records_with_extended_feedback)
    logging.info("completed")
