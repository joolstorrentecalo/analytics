"""
Run each class (which requests from its respective endpoint) from the command line
"""

import logging
import os
from typing import Type

import click
import thought_industries_api


def cls_factory(class_name_to_run: str) -> Type:
    """Returns instantiated class obj based on class string name"""
    cls_obj = getattr(thought_industries_api, class_name_to_run)
    return cls_obj()


def calculate_epoch():
    """
    Retrieve the epoch start/end dates environ variables.
    If the start_date is before epoch_threshold_backfill_all,
    set the start_date to 2010.

    The DAG start_date is 3/1/2022, but we need to retrieve
    data all the way back from when data started- 4/6/2020.

    However, between 4/6/20 and 3/1/22, there's very little data
    so rather than doing it in a seperate daily task, do it all
    in one task.
    """
    # add 1 ms so that there's no overlap between runs
    epoch_start_ms = (int(os.environ["EPOCH_START_STR"]) * 1000) + 1

    # if start_epoch ts is before threshold, backfill all
    # This should only happen on the first DAG run
    epoch_threshold_backfill_all = 1646175600000  # 3/1/2022
    if epoch_start_ms < epoch_threshold_backfill_all:
        logging.info(
            "epoch_start_ms '%s' from airflow is before threshold '%s' "
            "and is being switched to 1265041161000",
            epoch_start_ms,
            epoch_threshold_backfill_all,
        )
        epoch_start_ms = 1264982400000  # 2/1/2010

    epoch_end_ms = int(os.environ["EPOCH_END_STR"]) * 1000
    return epoch_start_ms, epoch_end_ms


@click.command()
@click.option("--class-name-to-run")
def execute_date_interval_endpoint(class_name_to_run: str):
    """
    dynamically create `date_interval_endpoint` class, then call its fetch_and_upload_data()

    The data_interval_endpoint class takes in epoch_start_ms/epoch_end_ms parameters
    """
    class_to_run = cls_factory(class_name_to_run)

    epoch_start_ms, epoch_end_ms = calculate_epoch()
    logging.info("\nstart EPOCH_START_MS: %s", epoch_start_ms)
    logging.info("\nstart EPOCH_END_MS : %s", epoch_end_ms)

    class_to_run.fetch_and_upload_data(epoch_start_ms, epoch_end_ms)


@click.command()
@click.option("--class-name-to-run")
def execute_cursor_endpoint(class_name_to_run: str):
    """
    dynamically create `cursor_endpoint` class, then call its fetch_and_upload_data()
    """
    class_to_run = cls_factory(class_name_to_run)
    class_to_run.fetch_and_upload_data()


if __name__ == "__main__":
    # set-up logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True

    # Add Click commands
    cli = click.Group()
    cli.add_command(execute_date_interval_endpoint)
    cli.add_command(execute_cursor_endpoint)

    # Run CLI
    cli()
