""" Test deprovision_users.py """

from argparse import Namespace
from unittest.mock import patch

import pytest
from deprovision_users import process_args, flatten_list_of_tuples, compare_users


@patch("deprovision_users.parse_arguments")
def test_process_args(mock_parse_arguments):
    """
    Test that args are processed correctly
    """
    users_to_remove = ["usertoadd1", "usertoadd2"]
    test_run = True
    mock_parse_arguments.return_value = Namespace(
        users_to_remove=users_to_remove,
        test_run=test_run,
    )

    parsed_args = process_args()
    assert parsed_args[0] == users_to_remove
    assert parsed_args[1] is test_run


@pytest.mark.parametrize(
    "test_tuple_list, expected_list",
    [
        ([(1,)], [1]),
        ([(2,)], [2]),
        ([(1, 2)], [1]),
    ],
)
def test_flatten_list_of_tuples(test_tuple_list, expected_list):
    """
    Test that each tuple's first element is flattened into a list
    """
    assert flatten_list_of_tuples(test_tuple_list) == expected_list


@pytest.mark.parametrize(
    "users_roles_yaml, users_snowflake, missing_users_in_roles",
    [
        ([], ["user1", "user2"], ["user1", "user2"]),
        (["user1", "user2"], ["user1", "user2"], []),
        (["user1", "user2"], [], []),
    ],
)
def test_compare_users(users_roles_yaml, users_snowflake, missing_users_in_roles):
    """
    Check that users_snowflake that are misisng in users_roles_yaml are returned
    Scenarios:
        1. two users in users_snowflake, none in roles_yaml, both are missing usres
        2. Two users in both users_snowflake and roles_yaml, nothing missing
        3. Two users in roles.yml, none in snowflake, return nothing
    """
    assert compare_users(users_roles_yaml, users_snowflake) == missing_users_in_roles
