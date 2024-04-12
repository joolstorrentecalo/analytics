"""
This module args.py parses the command line arguments provided by the user.
"""

import argparse
from utils_update_roles import get_user_changes


def get_users_added() -> list:
    """returns the users ADDED to the snowflake_users.yml file"""

    try:
        users_added = get_user_changes()[0]
    except IndexError as e:
        raise IndexError(
            f"Check that utils_snowflake_provisionin.get_user_changes() returns 2 lists, error: {e}"
        )
    return users_added


def get_users_removed() -> list:
    """returns the users REMOVED from the snowflake_users.yml file"""
    try:
        users_removed = get_user_changes()[1]
    except IndexError as e:
        raise IndexError(
            f"Check that utils_snowflake_provisionin.get_user_changes() returns 2 lists, error: {e}"
        )
    return users_removed


def get_default_databases_template() -> str:
    """unused currently because default is None (do not allocate dev_db)"""
    return """[
        {"{{ prod_database }}": {"shared": false}},
        {"{{ prep_database }}": {"shared": false}}
    ]"""


def get_default_roles_template() -> str:
    """Returns default roles template"""
    return '{"{{ username }}": {"member_of": ["snowflake_analyst"], "warehouses": ["dev_xs"]}}'


def get_default_users_template() -> str:
    """Returns default users template"""
    return '{"{{ username }}": {"can_login": true, "member_of": ["{{ username }}"]}}'


def parse_arguments() -> argparse.Namespace:
    """
    The user can pass in the following arguemnts:
        --users-to-add
        --users-to-remove
        --databases-template
        --roles-template
        --users-template

    All arguments are optional- if no args passed in,
    then default arguments for `roles` and `users` are used.
    """
    parser = argparse.ArgumentParser(description="Update roles.yml options")
    parser.add_argument(
        "-ua",
        "--users-to-add",
        nargs="+",
        type=str,
        default=get_users_added(),
        help="users to ADD to the roles.yml file",
    )
    parser.add_argument(
        "-ur",
        "--users-to-remove",
        nargs="+",
        type=str,
        default=get_users_removed(),
        help="users to REMOVE from the roles.yml file",
    )
    parser.add_argument(
        "-d",
        "--databases-template",
        default=None,
        help="Database values template- pass in a JSON string object",
    )
    parser.add_argument(
        "-r",
        "--roles-template",
        default=get_default_roles_template(),
        help="Role values template- pass in a JSON string object",
    )
    parser.add_argument(
        "-u",
        "--users-template",
        default=get_default_users_template(),
        help="User values template- pass in a JSON string object",
    )
    # by default, only print what would happen to roles.yml, but don't overwrite it
    parser.add_argument(
        "-t",
        "--test-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If test, only print the sql statements, rather than running them in Snowflake",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    databases = args.databases
    role = args.role
    user = args.user
