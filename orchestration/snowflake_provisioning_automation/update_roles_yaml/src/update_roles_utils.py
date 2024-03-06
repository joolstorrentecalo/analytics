""" util functions """

import os
import logging
import subprocess
import yaml

DATABASES_KEY = "databases"
ROLES_KEY = "roles"
USERS_KEY = "users"
abs_path = os.path.realpath(__file__)
YAML_PATH = abs_path[: abs_path.find("/orchestration")] + "/permissions/snowflake/"


class IndentDumper(yaml.Dumper):
    """
    Add appropriate indent when saving to yaml file
    source: https://reorx.com/blog/python-yaml-tips/#enhance-list-indentation-dump
    """

    def increase_indent(self, flow=False, indentless=False):
        """add indent"""
        return super().increase_indent(flow, False)


def get_roles_from_yaml():
    """read in roles.yml file as python data structure"""
    roles_file_name = "roles.yml"
    roles_file_path = os.path.join(YAML_PATH, roles_file_name)

    with open(roles_file_path, "r", encoding="utf-8") as stream:
        try:
            roles = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
    return roles


def save_roles_to_yaml(data):
    """
    Save data structure as YAML
    """
    roles_file_name = "roles.yml"
    roles_file_path = os.path.join(YAML_PATH, roles_file_name)
    with open(roles_file_path, "w", encoding="utf-8") as file:
        # safe_dump() cannot be used with Dumper arg
        yaml.dump(
            data,
            file,
            sort_keys=False,
            default_flow_style=False,
            Dumper=IndentDumper,
        )

    logging.info("roles.yml has been overwritten with updated data")


def run_git_diff_command(file_path, base_branch="master"):
    """Run git diff command and capture the output"""

    git_diff_command = f"""
    git diff {base_branch}  -- {file_path} \
    | grep '^[+-]' | grep -Ev '^(--- a/|\\+\\+\\+ b/|--- /dev/null)'
    """

    diff_output = subprocess.check_output(git_diff_command, shell=True, text=True)
    return diff_output


def get_username_changes():
    """
    Based on git diff to the `snowflake_usernames.yml` file,
    returns user additions and removals
    """
    base_branch = "master"
    # Get the directory of the Python script
    usernames_file_name = "snowflake_usernames.yml"
    usernames_file_path = os.path.join(YAML_PATH, usernames_file_name)

    # Run the Git diff command
    output = run_git_diff_command(usernames_file_path, base_branch)
    changes = output.split("\n")
    usernames_added = []
    usernames_removed = []

    for change in changes:
        if change.startswith("-"):
            usernames_added.append(change[3:])
        elif change.startswith("+"):
            usernames_removed.append(change[3:])
    return usernames_added, usernames_removed
