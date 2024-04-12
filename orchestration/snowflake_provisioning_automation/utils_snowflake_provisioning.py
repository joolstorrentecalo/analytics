"""
utils module for both `provision_users/` and `update_roles_yaml/`

Child modules access this utils file by running
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/update_roles_yaml")]
sys.path.insert(1, parent_path)

The `git diff` command could have been alternatively placed within
the CI job, but `get_username_changes()` logic is a little bit tricky
to do in bash.
Furthermore, by doing it in python, the entrypoint can be either CI job/python script
"""

import os
import subprocess
import re
import logging
from typing import Tuple, List


abs_path = os.path.realpath(__file__)
YAML_PATH = abs_path[: abs_path.find("/orchestration")] + "/permissions/snowflake/"
config_dict = os.environ.copy()


def run_git_diff_command(file_path: str, base_branch: str = "master") -> str:
    """Run git diff command and capture the output"""

    git_diff_command = f"git diff {base_branch}  -- {file_path}"

    # If git diff has output, then grep for modified lines
    # https://stackoverflow.com/a/26622262
    grep_diff_command = f"""
    if [ -n "$({git_diff_command})" ]; then \
        {git_diff_command} \
    | grep '^[+-]' | grep -Ev '^(--- a/|\\+\\+\\+ b/|--- /dev/null)'
    else
        echo ''
    fi
    """

    try:
        result = subprocess.run(
            grep_diff_command, capture_output=True, check=True, text=True, shell=True
        )
        diff_output = result.stdout
    except subprocess.CalledProcessError as e:
        raise e
    return diff_output


def get_snowflake_usernames(users):
    """
    Return snowflake username, need to update the string by:
    - Remove '-ext'
    - Remove non \\w chars
    """
    usernames = []
    for user in users:
        user = user.replace("-ext", "")
        user = user.split("@")[0]
        # Replace all non-word characters with blanks
        user = re.sub(r"\W+", "", user)
        usernames.append(user)
    return usernames


def get_emails(users):
    """
    From the user, i.e `jdoe-ext`, return the email
    """
    # for safety, in case user had added domain name into user argument
    users = [user.split("@")[0] for user in users]

    domain = config_dict["EMAIL_DOMAIN"]
    emails = []
    for user in users:
        email = f"{user}@{domain}"
        emails.append(email)
    return emails


def check_is_valid_user_format(user):
    """
    To prevent sql injection, make sure the `user` string
    matches the following criteria
    Characters must be: a-z, A-Z, 0-9, or '-'
    """
    pattern = re.compile(
        "[^a-zA-Z0-9-]|-{2,}"
    )  # Matches any character not a-z, A-Z, 0-9, or two or more consecutive hyphens

    # Check if the pattern matches any part of the user variable
    if pattern.search(user):
        return False
    return True


def get_user_changes() -> Tuple[List[str], List[str]]:
    """
    Based on git diff to the `snowflake_users.yml` file,
    returns user additions and removals
    """
    # Get the directory of the Python script
    users_file_name = "snowflake_users.yml"
    users_file_path = os.path.join(YAML_PATH, users_file_name)

    # Run the Git diff command
    base_branch = "origin/master"
    diff_output = run_git_diff_command(users_file_path, base_branch)

    users_added = []
    users_removed = []

    for change in diff_output.split("\n"):
        try:
            user = change[3:]
        except IndexError:
            continue

        # check that user isn't a blank line
        if (change.startswith("+") or change.startswith("-")) and user:
            if not check_is_valid_user_format(user):
                logging.info(f"Skipping user {user}, not a valid user")
                continue
            if change.startswith("+"):
                users_added.append(user)
            if change.startswith("-"):
                users_removed.append(user)

    return users_added, users_removed
