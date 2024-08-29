"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""

import sys
from datetime import datetime, timedelta
from logging import basicConfig, info

from data_classification import DataClassification
from fire import Fire


def run_extract(
    operation: str,
    date_from: str,
    unset: str = "FALSE",
    tagging_type: str = "INCREMENTAL",
    incremental_load_days: int = 90,
):
    """
    Run process
    """

    data_classification = DataClassification(
        tagging_type=tagging_type,
        mnpi_raw_file="mnpi_models.json",
        incremental_load_days=incremental_load_days,
    )
    if not date_from:
        curr_date = datetime.now() - timedelta(
            days=data_classification.incremental_load_days
        )
        date_from = curr_date.strftime("%Y-%m-%d 00:00:00")

    if operation == "EXTRACT":
        data_classification.extract()
    if operation == "CLASSIFY":
        data_classification.classify(
            date_from=date_from, unset=unset, tagging_type=tagging_type
        )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    info("START data classification.")
    Fire(run_extract)
    info("END with data classification.")
