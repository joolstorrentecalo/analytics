"""
Test utils file, most notably
that the correct usernames are returned based off the git diff.
"""

from unittest.mock import patch
from utils_snowflake_provisioning import (
    YAML_PATH,
    get_user_changes,
    get_snowflake_usernames,
    get_emails,
    check_is_valid_user_format,
)


def test_imports():
    """test that YAML_PATH variable was imported correctly"""
    assert YAML_PATH is not None


@patch("utils_snowflake_provisioning.run_git_diff_command")
def test_get_user_changes(mock_run_git_diff_command):
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
    users_added, users_removed = get_user_changes()

    assert users_added[0] == added_user
    assert users_removed[0] == removed_user


def test_get_snowflake_usernames():
    cleaned_username = "jdoe"

    # regular user
    users = ["jdoe"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username

    # external user
    users = ["jdoe-ext"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username

    # users with non \w char
    users = ["j-doe"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username

    # users with non \w char, part 2
    users = ["j.doe"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username

    # users with non \w char, part 2
    users = ["jdoe@gitlab.com"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username

    # users with non \w char, part 2
    users = ["j.doe-ext@gitlab.com"]
    usernames = get_snowflake_usernames(users)
    assert usernames[0] == cleaned_username


def test_get_emails():
    """
    From the user, i.e `jdoe-ext`, return the email
    """
    users = ["jdoe", "jdoe-ext", "jdoe@gitlab.com"]
    emails = get_emails(users)
    assert emails == ["jdoe@gitlab.com", "jdoe-ext@gitlab.com", "jdoe@gitlab.com"]


def test_check_is_valid_user_format():
    """Test check_is_valid_user_format() logic"""
    # regular user, should be true
    user = "jdoe"
    is_valid_user = check_is_valid_user_format(user)
    assert is_valid_user is True

    # another regular user, should be true
    user = "jdoe-ext"
    is_valid_user = check_is_valid_user_format(user)
    assert is_valid_user is True

    # two '-', should be false
    user = "jdoe--ext"
    is_valid_user = check_is_valid_user_format(user)
    assert is_valid_user is False

    # '.' is invalid char, should be False
    user = "jdoe.ext"
    is_valid_user = check_is_valid_user_format(user)
    assert is_valid_user is False

    # sql injection attempt, should be False
    user = "jdoe; drop table some_table; -- some other query"
    is_valid_user = check_is_valid_user_format(user)
    assert is_valid_user is False
