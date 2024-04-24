"""
Test utils file, most notably
that the correct usernames are returned based off the git diff.
"""

import pytest
from unittest.mock import patch
from utils_snowflake_provisioning import (
    YAML_PATH,
    USERS_FILE_NAME,
    get_file_changes,
    get_snowflake_usernames,
    get_emails,
    check_is_valid_user_format,
    get_valid_users,
)


def test_imports():
    """test that YAML_PATH variable was imported correctly"""
    assert YAML_PATH is not None


@patch("utils_snowflake_provisioning.run_git_diff_command")
def test_get_file_changes(mock_run_git_diff_command):
    """
    Test that given a diff it returns the correct is_valid_userults.
    Specifically lines with a '+' belong to usernames_added
    and lines with a '-' belong to usernames_removed

    Note that modified blank lines are ignored
    """
    added_user = "addeduser"
    removed_user = "removeduser"
    test_diff = f"+\n+--{added_user}\n---{removed_user}"
    mock_run_git_diff_command.return_value = test_diff
    users_added, users_removed = get_file_changes(YAML_PATH, USERS_FILE_NAME)

    assert users_added[0] == added_user
    assert users_removed[0] == removed_user


@pytest.mark.parametrize(
    "test_usernames, expected_usernames",
    [
        (
            [
                "jdoe",
                "jdoe-ext",
                "j-doe",
                "j.doe",
                "jdoe@gitlab.com",
                "j.doe-ext@gitlab.com",
            ],
            ["jdoe", "jdoe", "jdoe", "jdoe", "jdoe", "jdoe"],
        )
    ],
)
def test_get_snowflake_usernames(test_usernames, expected_usernames):
    """Test that users are converted to valid snowflake usernames"""
    assert get_snowflake_usernames(test_usernames) == expected_usernames


@pytest.mark.parametrize(
    "test_emails, expected_emails",
    [
        (
            ["jdoe", "jdoe-ext", "jdoe@gitlab.com"],
            ["jdoe@gitlab.com", "jdoe-ext@gitlab.com", "jdoe@gitlab.com"],
        )
    ],
)
def test_get_emails(test_emails, expected_emails):
    """
    From the user, i.e `jdoe-ext`, return the email
    """
    assert get_emails(test_emails) == expected_emails


@pytest.mark.parametrize(
    "test_user, expected_is_valid",
    [
        ("jdoe", True),
        ("jdoe-ext", True),
        ("jdoe--ext", False),
        ("jdoe.ext", False),
        ("jdoe; drop table some_table; -- some other query", False),
    ],
)
def test_check_is_valid_user_format(test_user, expected_is_valid):
    """Test check_is_valid_user_format() logic"""

    assert check_is_valid_user_format(test_user) is expected_is_valid


@pytest.mark.parametrize(
    "test_users, expected_users",
    [
        (
            [
                "jdoe",
                "jdoe-ext",
                "jdoe--ext",
                "jdoe.ext",
                "jdoe; drop table some_table; -- some other query",
            ],
            ["jdoe", "jdoe-ext"],
        )
    ],
)
def test_get_valid_users(test_users, expected_users):
    """
    Test that only valid users are returned
    """
    assert get_valid_users(test_users) == expected_users
