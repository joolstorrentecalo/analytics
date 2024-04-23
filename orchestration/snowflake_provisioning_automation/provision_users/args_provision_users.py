"""
This module args.py parses the command line arguments provided by the user.
"""

import os
import sys
import argparse

# needed to import shared utils module
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/provision_users")]
sys.path.insert(1, parent_path)
from utils_snowflake_provisioning import get_file_changes, YAML_PATH, USERS_FILE_NAME


def get_users_added() -> list:
    """returns the users ADDED to the snowflake_users.yml file"""
    try:
        users_added = get_file_changes(YAML_PATH, USERS_FILE_NAME)[0]
    except IndexError as e:
        raise IndexError(
            f"Check that utils_snowflake_provisionin.get_file_changes() returns 2 lists, error: {e}"
        )
    return users_added


def parse_arguments() -> argparse.Namespace:
    """
    The user can pass in the following arguemnts:
        --users-to-add
        --test-run
        --dev-db

    All arguments are optional- if no args passed in,
    then default arguments for `roles` and `users` are used.
    """
    parser = argparse.ArgumentParser(description="Provision users in Snowflake options")
    parser.add_argument(
        "-ua",
        "--users-to-add",
        nargs="+",
        type=str,
        default=get_users_added(),
        help="users to ADD to the roles.yml file",
    )
    # by default, only print snowflake queries, don't run in Snowflake
    parser.add_argument(
        "-t",
        "--test-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If test, only print the sql statements, rather than running them in Snowflake",
    )
    # if arg not selected, will not run with dev-db
    parser.add_argument(
        "-db",
        "--dev-db",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="If selected, create development databases for each user",
    )

    return parser.parse_args()
