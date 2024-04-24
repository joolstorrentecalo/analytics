"""
Make sure that provision_users main script runs without any errors.
"""

import sys
from argparse import Namespace
from provision_users import main, process_args
from unittest.mock import patch


def test_run_provision_users():
    """Check that provision_users.main() runs without errors"""
    original_argv = sys.argv
    # by default argparse checks sys.argv[1:], which in this case will be blank
    sys.argv = ["some_placeholder"]
    try:
        main()
    except:
        raise

    # Restore original sys.argv
    sys.argv = original_argv


@patch("provision_users.parse_arguments")
def test_process_args(mock_parse_arguments):
    """
    Test that args are processed correctly
    Specifically that `invalid_user_to_add` was correctly dropped
    """
    test_run = True
    dev_db = False
    mock_parse_arguments.return_value = Namespace(
        users_to_add=["usertoadd", "invalid_user_to_add"],
        dev_db=dev_db,
        test_run=test_run,
    )

    parsed_args = process_args()
    assert parsed_args[0] == ["usertoadd"]
    assert parsed_args[1] is dev_db
    assert parsed_args[2] is test_run
