"""
This module args.py parses the command line arguments provided by the user.
"""

import argparse


def parse_arguments() -> argparse.Namespace:
    """
    The user can pass in the following arguemnts:
        --users-to-remove
        --test-run

    All arguments are optional- if no args passed in,
    then default for users is used.
    The default user value will be all users in Snowflake
    that are missing in roles.yml
    """
    parser = argparse.ArgumentParser(
        description="Deprovision users in Snowflake options"
    )
    parser.add_argument(
        "-ur",
        "--users-to-remove",
        nargs="+",
        type=str,
        default=None,
        help="users to remove from Snowflake",
    )
    # by default, only print snowflake queries, don't run in Snowflake
    parser.add_argument(
        "-t",
        "--test-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If test, only print the sql statements, rather than running them in Snowflake",
    )
    return parser.parse_args()
