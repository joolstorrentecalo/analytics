""" util functions """

import sys
import os
import logging
from typing import Union

import yaml

# needed to import shared utils module
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/update_roles_yaml")]
sys.path.insert(1, parent_path)
from utils_snowflake_provisioning import (
    get_user_changes,
    YAML_PATH,  # used by downstream modules
    get_snowflake_usernames,
)

# imported by other modules
DATABASES_KEY = "databases"
ROLES_KEY = "roles"
USERS_KEY = "users"
abs_path = os.path.realpath(__file__)


class IndentDumper(yaml.Dumper):
    """
    Add appropriate indent when saving to yaml file
    source: https://reorx.com/blog/python-yaml-tips/#enhance-list-indentation-dump
    """

    def increase_indent(self, flow=False, indentless=False):
        """add indent"""
        return super().increase_indent(flow, False)


def get_roles_from_yaml() -> Union[dict, list]:
    """read in roles.yml file as python data structure"""
    roles_file_name = "roles.yml"
    roles_file_path = os.path.join(YAML_PATH, roles_file_name)

    with open(roles_file_path, "r", encoding="utf-8") as stream:
        try:
            roles_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
    return roles_data


def save_roles_to_yaml(data: Union[dict, list]):
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
