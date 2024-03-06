"""
This is the main module, on a high level, remove or add users to roles.yml
based on what is passed in via command line arguments.

There are two main dimensions that the end_user needs to think about:
    - users to add/remove
    - for each user to be added, what corresponding values need to be added.

For both of these dimensions, there are default options, and custom options.

For users, the default option is based on any changes
to `permissions/snowflake/snowflake_usernames.yml`.
For custom options, the end_user can pass in a custom list of users.

For user values (databases, roles, users)
There are default values for `roles` and `users`.
For example, default role will be `snowflake_analyst`.

For custom values, the user can pass in any valid json as a templated string.
It needs to be templated so that the template can be rendered with the `username`.
For specific instructions, please see the handbook.
"""

import logging
from typing import Tuple
from args import parse_arguments
from update_roles_utils import (
    DATABASES_KEY,
    ROLES_KEY,
    USERS_KEY,
    get_roles_from_yaml,
    save_roles_to_yaml,
)
from render_templates import concat_template_values
from roles_struct import RolesStruct


def configure_logging():
    """configure logger"""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def process_args() -> Tuple[list, list, str, str, str]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.usernames_to_remove,
        args.usernames_to_add,
        args.databases_template,
        args.roles_template,
        args.users_template,
    )


def add_user_values(
    roles_yaml: str, yaml_key: str, usernames_to_add: list, template: str
):
    """
    Adds users and their values to the the roles_yaml data structure:
    First converts the values template to a dict
    Then creates a RolesStruct object
    Finally, using the instantiated object, update the roles_yaml data structure.
    """
    values = concat_template_values(usernames_to_add, template)
    roles_struct = RolesStruct(roles_yaml, yaml_key, values)
    roles_struct.add_values()


def add_users(
    roles_yaml: str,
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
        add_user_values(roles_yaml, DATABASES_KEY, usernames_to_add, databases_template)

    if roles_template:
        add_user_values(roles_yaml, ROLES_KEY, usernames_to_add, roles_template)

    if users_template:
        add_user_values(roles_yaml, USERS_KEY, usernames_to_add, users_template)


def remove_users(roles_yaml: str, usernames_to_remove: list):
    """Remove each username from roles.yml"""
    remove_struct = RolesStruct(roles_yaml, usernames_to_remove=usernames_to_remove)
    remove_struct.remove_values()


def main():
    """entrypoint function"""
    configure_logging()
    roles_yaml = get_roles_from_yaml()
    (
        usernames_to_remove,
        usernames_to_add,
        databases_template,
        roles_template,
        users_template,
    ) = process_args()
    logging.info(f"usernames_to_add: {usernames_to_add}")
    logging.info(f"usernames_to_remove: {usernames_to_remove}\n")

    if usernames_to_add:
        add_users(
            roles_yaml,
            usernames_to_add,
            databases_template,
            roles_template,
            users_template,
        )

    if usernames_to_remove:
        remove_users(roles_yaml, usernames_to_remove)

    save_roles_to_yaml(roles_yaml)


if __name__ == "__main__":
    main()
