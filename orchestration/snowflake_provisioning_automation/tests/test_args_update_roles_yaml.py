"""
Test cases for the following 6 arguments:
    - --users-to-add
    - --users-to-remove
    - --databases-template
    - --roles-template
    - --users-template
    - --test-run
"""

from unittest.mock import patch
from args_update_roles_yaml import (
    parse_arguments,
    get_default_roles_template,
    get_default_users_template,
    get_users_added,
    get_users_removed,
)


def test_parse_arguments_defaults():
    """Run with all default arguments"""
    with patch("sys.argv", ["some_test_script"]):
        args = parse_arguments()
    assert args.users_to_add == get_users_added()
    assert args.users_to_remove == get_users_removed()
    assert args.databases_template is None
    assert args.roles_template == get_default_roles_template()
    assert args.users_template == get_default_users_template()
    assert args.test_run is True


def test_parse_arguments_with_users_to_add():
    """Test users_to_add argument"""
    users_str = "user1 user2"
    args_list = ["-ua", users_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.users_to_add == [users_str]


def test_parse_arguments_with_users_to_remove():
    """Test users_to_remove argument"""
    users_str = "user3 user4"
    args_list = ["-ur", users_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.users_to_remove == [users_str]


def test_parse_arguments_with_databases_template():
    """Test databases_template argument"""
    database_template_str = "{databases: some_db_value}"
    args_list = ["--databases-template", database_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.databases_template == database_template_str


def test_parse_arguments_with_roles_template():
    """Test roles_template argument"""
    roles_template_str = "{roles: some_roles_value}"
    args_list = ["--roles-template", roles_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.roles_template == roles_template_str


def test_parse_arguments_with_users_template():
    """Test users_template argument"""
    users_template_str = "{users: some_users_value}"
    args_list = ["--users-template", users_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.users_template == users_template_str


def test_parse_arguments_with_databases_template_blank_args():
    """
    Test databases_template BLANK argument, should return default template
    """
    database_template_str = ""
    args_list = ["--databases-template", database_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.databases_template is None


def test_parse_arguments_with_roles_template_blank_args():
    """
    Test roles_template BLANK argument, should return default template
    """
    roles_template_str = ""
    args_list = ["--roles-template", roles_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.roles_template == get_default_roles_template()


def test_parse_arguments_with_users_template_blank_args():
    """
    Test users_template BLANK argument, should return default template
    """
    users_template_str = ""
    args_list = ["--users-template", users_template_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.users_template == get_default_users_template()


def test_parse_arguments_test_run():
    args_list = ["--test-run"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.test_run is True


def test_parse_arguments_no_test_run():
    args_list = ["--no-test-run"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.test_run is False
