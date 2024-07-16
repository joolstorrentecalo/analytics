"""
This process is used to deprovision users in Snowflake.
Users are deprovisioned if they are no longer in `roles.yml`.

Unlike the rest of the processes in `snowflake_provisioning_automation/`,
this process is not accessible CI job.
This is because there's no immediate rush to delete users,
and also because deleting users is more sensitive, it will be an
automatic job.

The entrypoint of this job will be from
Airflow DAG snowflake_cleanup.py
"""

import os
import sys
import logging
import time
from typing import Tuple

from snowflake_connection import (
    SnowflakeConnection,
    get_securityadmin_connection,
    get_sysadmin_connection,
)
from provision_users import _provision
from args_deprovision_users import parse_arguments

# need to import update_roles_yaml module
abs_path = os.path.dirname(os.path.realpath(__file__))
roles_yaml_path = os.path.join(
    abs_path[: abs_path.find("/provision_users")], "update_roles_yaml/"
)
sys.path.insert(1, roles_yaml_path)
from utils_update_roles import (
    USERS_KEY,
    ROLES_KEY,
    get_roles_from_url,
    configure_logging,
)
from roles_struct import RolesStruct


def process_args() -> Tuple[list, list, str, str, str]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.users_to_remove,
        args.test_run,
    )


def flatten_list_of_tuples(tuple_list: list) -> list:
    """
    Converts a list of tuples into a list
    Only gets the first element of each tuple
    tuple_list example: [(user1, ), (user2, )]
    """
    try:
        flattened_list = [t[0] for t in tuple_list]
    except IndexError:
        logging.info("No records returned from Snowflake users query")
        raise
    return flattened_list


def compare_users(users_roles_yaml: list, users_snowflake: list) -> list:
    """
    Return the users that are in users_snowflake list
    that are missing in users_roles_yaml list
    """
    missing_users_in_roles = [
        user for user in users_snowflake if user not in users_roles_yaml
    ]
    return missing_users_in_roles


def get_users_from_roles_yaml() -> list:
    """
    Make request to roles.yml within master branch
    Obtain the existing users and roles within that file
    Return the combined users and roles.
    Combining users and roles ensures that no Snowflake objects
    are incorrectly dropped.
    """
    roles_data = get_roles_from_url()
    roles_struct = RolesStruct(roles_data)
    users_roles_yaml = roles_struct.get_existing_value_keys(USERS_KEY)
    roles_roles_yaml = roles_struct.get_existing_value_keys(ROLES_KEY)
    users_and_roles = list(set(users_roles_yaml + roles_roles_yaml))
    return users_and_roles


def get_users_snowflake(connection: SnowflakeConnection) -> list:
    """
    Get users from Snowflake by querying `Users` table
    The query has the following conditions:
        - does not have a password- indicates not a service account
        - the user was created more than a week ago
            - don't want to remove users that were added to snowflake
            but roles.yml MR not merged yet
    """
    query = """
    SELECT lower(name) as name
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
    WHERE true
    and has_password = false
    and deleted_on is NULL
    and created_on < DATEADD(WEEK, -1, CURRENT_TIMESTAMP)
    ORDER BY name;
    """
    users_tuple_list = connection.run_sql_statement(query)
    users_snowflake = flatten_list_of_tuples(users_tuple_list)
    return users_snowflake


def get_users_to_remove(connection: SnowflakeConnection) -> list:
    """
    Obtain list of all users from roles.yml and Snowflake
    Drop any users in Snowflake that are no longer in roles.yml
    """
    users_roles_yaml = get_users_from_roles_yaml()
    users_snowflake = get_users_snowflake(connection)

    missing_users = compare_users(users_roles_yaml, users_snowflake)
    return missing_users


def deprovision_users(connection: SnowflakeConnection, usernames: list):
    """
    Deprovision users in Snowflake by running DROP users and roles
    """
    template_filename = "deprovision_user.sql"
    logging.info("#### Deprovisioning users ####")
    _provision(connection, template_filename, usernames)


def main():
    """entrypoint function"""
    configure_logging()

    users_to_remove_arg, is_test_run = process_args()
    logging.info(f"is_test_run: {is_test_run}")
    time.sleep(5)  # give user a chance to abort

    # if users are passed in to remove, remove those users
    if users_to_remove_arg:
        users_to_remove = users_to_remove_arg

    # else if users were NOT passed in to remove
    # then check if any snowflake_users are missing in roles.yml
    else:
        sysadmin_connection = get_sysadmin_connection(is_test_run=False)
        users_to_remove = get_users_to_remove(sysadmin_connection)
        sysadmin_connection.dispose_engine()
    #
    logging.info(f"users_to_remove: {users_to_remove}")

    securityadmin_connection = get_securityadmin_connection(is_test_run)
    deprovision_users(securityadmin_connection, users_to_remove)
    securityadmin_connection.dispose_engine()


if __name__ == "__main__":
    main()
