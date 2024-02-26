"""
 This is main code base to kick of the ticket and ticket_audit transformation.
"""
import os
import sys
from logging import basicConfig, getLogger, info

import fire

from zendesk_ticket_audits_refactor import refactor_ticket_audits_read_gcs
from zendesk_tickets_refactor import refactor_tickets_read_gcs

config_dict = os.environ.copy()

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(
        {
            "refactor_ticket_audits": refactor_ticket_audits_read_gcs,
            "refactor_tickets": refactor_tickets_read_gcs,
        }
    )
    info("Complete.")
