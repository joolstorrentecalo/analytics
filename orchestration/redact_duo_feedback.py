import json
import logging
import sys
from os import environ as env
from typing import List, Dict

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

def get_records_with_extended_feedback(engine: Engine) -> List[str]:
    """
    retrieves snowplow events with Duo extended feedback populated
    """

    query = """
    SELECT event_id, contexts
    FROM testing_db.test.snowplow_gitlab_events_clone
    WHERE se_label ='response_feedback'
--  AND contexts like '%"extendedFeedback":%'
    """

    try:
        logging.info("Getting snowplow events with extended feedback")
        connection = engine.connect()
        duo_feedback_events = [row[0] for row in connection.execute(query).fetchall()]
    except:
        logging.info("Failed to get snowplow events")
    finally:
        connection.close()
        engine.dispose()

    return duo_feedback_events

def redact_extended_feedback():
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    records = get_records_with_extended_feedback(engine)
    print(records)

if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(redact_extended_feedback())
    logging.info(duo_feedback_events)