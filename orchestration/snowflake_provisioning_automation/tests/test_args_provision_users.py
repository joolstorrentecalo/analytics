"""
Test cases for the following 3 arguments:
    - --users-to-add
    - --test-run
    - --dev-db
"""

from unittest.mock import patch
from args_provision_users import parse_arguments, get_users_added


def test_parse_arguments_defaults():
    """Run with all default arguments"""
    with patch("sys.argv", ["some_test_script"]):
        args = parse_arguments()
    assert args.users_to_add == get_users_added()
    assert args.test_run is True
    assert args.dev_db is False


def test_parse_arguments_with_users_to_add():
    """Test users_to_add argument"""
    users_str = "user1 user2"
    args_list = ["-ua", users_str]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.users_to_add == [users_str]


def test_parse_arguments_test_run_true():
    """Test --test-run"""
    args_list = ["--test-run"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.test_run is True


def test_parse_arguments_no_test_run():
    """Test --no-test-run"""
    args_list = ["--no-test-run"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.test_run is False


def test_parse_arguments_dev_db():
    """Test --dev-db"""
    args_list = ["--dev-db"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.dev_db is True


def test_parse_arguments_no_dev_db():
    """Test --no-dev-db"""
    args_list = ["--no-dev-db"]
    with patch("sys.argv", ["some_test_script"] + args_list):
        args = parse_arguments()
    assert args.dev_db is False
