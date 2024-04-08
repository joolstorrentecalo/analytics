"""
In Snowflake, create the user/roles/grants needed.

There are 2 main things that need to be done:
    - for users being added:
        - 1) create the user and role
        - 2) optionally, create their development databases

Originally, there was also the option to remove users from Snowflake.
However, this will be done in a separate process to eliminate any security risks/accidents.

Each of these actions is done via reading in a templated sql script,
rendering the script with the specified user,
and running the sql script via sqlalchemy connection.
"""

import sys
import os
import logging

import time
from typing import Tuple
from sqlalchemy.engine.base import Engine
from jinja2 import Template

from args_provision_users import parse_arguments
from snowflake_connection import SnowflakeConnection
from convert_sql_templates import convert_to_sql_statements

# needed to import shared utils module
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/provision_users")]
sys.path.insert(1, parent_path)
from utils_snowflake_provisioning import get_snowflake_usernames, get_emails


def configure_logging():
    """configure logger"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def process_args() -> Tuple[list, list, bool, bool]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.users_to_add,
        args.dev_db,
        args.test_run,
    )


def _get_snowflake_connection(role: str, is_test_run: bool):
    """helper method to return snowflake_connection for particular role"""
    config_dict = os.environ.copy()
    return SnowflakeConnection(config_dict, role, is_test_run)


def get_securityadmin_connection(is_test_run: bool):
    """return securityadmin snowflake connection"""
    role = "SECURITYADMIN"
    return _get_snowflake_connection(role, is_test_run)


def get_sysadmin_connection(is_test_run: bool):
    """return sysadmin snowflake connection"""
    role = "SYSADMIN"
    return _get_snowflake_connection(role, is_test_run)


def _provision(
    connection: Engine, template_filename: str, usernames: list, emails: list = None
):
    """
    All provision types (new users, new databases, deprovision users)
    run this base method.

    The template is rendered into a series of string sql statements
    for each passed in user.
    Each sql statement is then run in Snowflake.
    """
    sql_statements = convert_to_sql_statements(template_filename)
    for i in range(len(usernames)):
        logging.info(f"username: {usernames[i]}")
        query_params = {
            "username": usernames[i],
            "email": emails[i] if emails else None,
        }
        connection.run_sql_statements(sql_statements, query_params)


def provision_users(connection: Engine, usernames: list, emails: list):
    """provision user in Snowflake"""
    template_filename = "provision_user.sql"
    logging.info("#### Provisioning users ####")
    _provision(connection, template_filename, usernames, emails)


def provision_databases(connection: Engine, usernames: list):
    """provision personal databases in Snowflake"""
    template_filename = "provision_database.sql"
    logging.info("#### Provisioning user databases ####")
    _provision(connection, template_filename, usernames)


def deprovision_users(connection: Engine, usernames: list):
    """
    Deprovision users in Snowflake
    Currently unused, will do Snowflake deprovision in separate process
    """
    template_filename = "deprovision_user.sql"
    logging.info("#### Deprovisioning users ####")
    _provision(connection, template_filename, usernames)


def provision_all():
    """
    Performs the following actions:
        - provision users
        - provision databases
        - deprovision users
    """
    users_to_add, is_dev_db, is_test_run = process_args()
    emails_to_add = get_emails(users_to_add)
    usernames_to_add = get_snowflake_usernames(users_to_add)

    logging.info(f"provision users Snowflake, is_test_run: {is_test_run}\n")
    logging.info(f"usernames_to_add: {usernames_to_add}")
    logging.info(f"is_dev_db: {is_dev_db}")
    time.sleep(5)  # give user a chance to abort

    securityadmin_connection = get_securityadmin_connection(is_test_run)
    sysadmin_connection = get_sysadmin_connection(is_test_run)

    provision_users(securityadmin_connection, usernames_to_add, emails_to_add)
    if is_dev_db:
        provision_databases(sysadmin_connection, usernames_to_add)


def main():
    """entrypoint"""
    configure_logging()
    provision_all()


if __name__ == "__main__":
    main()
