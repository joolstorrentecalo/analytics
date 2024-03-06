"""
This module args.py parses the commandline arguments provided by the user.
"""

import argparse
from update_roles_utils import get_username_changes


def get_usernames_added() -> list:
    """returns the usernames ADDED to the snowflake_usernames.yml file"""
    return get_username_changes()[0]


def get_usernames_removed() -> list:
    """returns the usernames REMOVED from the snowflake_usernames.yml file"""
    return get_username_changes()[1]


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


def parse_arguments():
    """
    The user can pass in the following arguemnts:
        --usernames-to-add
        --usernames-to-remove
        --databases-template
        --roles-template
        --users-template

    All arguments are optional- if no args passed in,
    then default arguments for `roles` and `users` are used.
    """
    parser = argparse.ArgumentParser(description="Update roles.yml options")
    parser.add_argument(
        "-ua",
        "--usernames-to-add",
        nargs="+",
        type=str,
        default=get_usernames_added(),
        help="usernames to ADD to the roles.yml file",
    )
    parser.add_argument(
        "-ur",
        "--usernames-to-remove",
        nargs="+",
        type=str,
        default=get_usernames_removed(),
        help="usernames to REMOVE from the roles.yml file",
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

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    databases = args.databases
    role = args.role
    user = args.user
