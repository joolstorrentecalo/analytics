"""
Make sure that update_roles_yaml main script runs without any errors.
"""

import sys
from update_roles_yaml import main, process_args
from argparse import Namespace
from unittest.mock import patch


def test_run_update_roles_yaml():
    """Check that update_roles_yaml.main() runs without errors"""
    original_argv = sys.argv
    # by default argparse checks sys.argv[1:], which in this case will be blank
    sys.argv = ["some_placeholder"]
    try:
        main()
    except:
        raise

    # Restore original sys.argv
    sys.argv = original_argv


@patch("update_roles_yaml.parse_arguments")
def test_process_args(mock_parse_arguments):
    """
    Test that args are processed correctly
    Specifically that `invalid_user_to_add` was correctly dropped
    """
    databases_template = "some_db_template"
    roles_template = "some_roles_template"
    users_template = "some_users_template"
    test_run = True

    mock_parse_arguments.return_value = Namespace(
        users_to_add=["usertoadd", "invalid_user_to_add"],
        users_to_remove=["usertoremove", "invalid_user_to_remove"],
        databases_template=databases_template,
        roles_template=roles_template,
        users_template=users_template,
        test_run=test_run,
    )

    parsed_args = process_args()
    assert parsed_args[0] == ["usertoadd"]
    assert parsed_args[1] == ["usertoremove"]
    assert parsed_args[2] == databases_template
    assert parsed_args[3] == roles_template
    assert parsed_args[4] == users_template
    assert parsed_args[5] == test_run
