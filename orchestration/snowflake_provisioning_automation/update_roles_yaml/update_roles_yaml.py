"""
This is the main module, on a high level, remove or add users to roles.yml
based on what is passed in via command line arguments.

There are two main dimensions that the end_user needs to think about:
    - users to add/remove
    - for each user to be added, what corresponding values need to be added.

For both of these dimensions, there are default options, and custom options.

For users, the default option is based on any changes
to `permissions/snowflake/snowflake_users.yml`.
For custom options, the end_user can pass in a custom list of users.

For user values (databases, roles, users)
There are default values for `roles` and `users`.
For example, default role will be `snowflake_analyst`.

For custom values, the user can pass in any valid json as a templated string.
It needs to be templated so that the template can be rendered with the `username`.
For specific instructions, please see the handbook.
"""

import logging
import time

from typing import Tuple

from args_update_roles_yaml import parse_arguments
from utils_update_roles import (
    DATABASES_KEY,
    ROLES_KEY,
    USERS_KEY,
    get_roles_from_yaml,
    save_roles_to_yaml,
    get_valid_users,
    get_snowflake_usernames,
    configure_logging,
)
from render_templates import concat_template_values
from roles_struct import RolesStruct


def process_args() -> Tuple[list, list, str, str, str]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    valid_users_to_add = get_valid_users(args.users_to_add)
    valid_users_to_remove = get_valid_users(args.users_to_remove)

    parsed_args = (
        valid_users_to_add,
        valid_users_to_remove,
        args.databases_template,
        args.roles_template,
        args.users_template,
        args.test_run,
    )
    return parsed_args


def add_username_values(
    roles_data: dict, yaml_key: str, usernames_to_add: list, template: str
):
    """
    Adds users and their values to the the roles_data data structure:
    First converts the values template to a dict
    Then creates a RolesStruct object
    Finally, using the instantiated object, update the roles_data data structure.
    """
    values = concat_template_values(usernames_to_add, template)
    roles_struct = RolesStruct(roles_data, yaml_key, values)
    roles_struct.add_values()


def add_usernames(
    roles_data: dict,
    usernames_to_add: list,
    databases_template: str,
    roles_template: str,
    users_template: str,
):
    """
    Adds users and their values for databases/roles/users
    If template is not supplied, it means by default
    it should be skipped, or the user did not pass it in via command line.
    """
    if databases_template:
        add_username_values(
            roles_data, DATABASES_KEY, usernames_to_add, databases_template
        )

    if roles_template:
        add_username_values(roles_data, ROLES_KEY, usernames_to_add, roles_template)

    if users_template:
        add_username_values(roles_data, USERS_KEY, usernames_to_add, users_template)


def remove_usernames(roles_data: dict, usernames_to_remove: list):
    """Remove each username from roles.yml"""
    remove_struct = RolesStruct(roles_data, usernames_to_remove=usernames_to_remove)
    remove_struct.remove_values()


def main():
    """entrypoint function"""
    configure_logging()
    roles_data = get_roles_from_yaml()
    (
        users_to_add,
        users_to_remove,
        databases_template,
        roles_template,
        users_template,
        is_test_run,
    ) = process_args()

    usernames_to_add = get_snowflake_usernames(users_to_add)
    usernames_to_remove = get_snowflake_usernames(users_to_remove)

    logging.info(f"update_roles_yaml is_test_run: {is_test_run}")
    time.sleep(5)  # give user a chance to abort
    logging.info(f"usernames_to_add: {usernames_to_add}")
    logging.info(f"usernames_to_remove: {usernames_to_remove}\n")
    logging.info(f"databases_template: {databases_template}")
    logging.info(f"roles_template: {roles_template}")
    logging.info(f"users_template: {users_template}")

    if usernames_to_add:
        add_usernames(
            roles_data,
            usernames_to_add,
            databases_template,
            roles_template,
            users_template,
        )

    if usernames_to_remove:
        remove_usernames(roles_data, usernames_to_remove)

    if not is_test_run and (usernames_to_add or usernames_to_remove):
        save_roles_to_yaml(roles_data)


if __name__ == "__main__":
    main()
