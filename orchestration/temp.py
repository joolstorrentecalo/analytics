

#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict, List
import urllib.request
from yaml import load, safe_load, YAMLError

# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)

@staticmethod
def get_roles() -> list:
    """
    retrieves snowflake roles from roles.yml
    """

    roles_yaml_url = "https://gitlab.com/gitlab-data/analytics/-/raw/master/permissions/snowflake/roles.yml"

    with urllib.request.urlopen(roles_yaml_url) as data:
        try:
            roles_yaml = safe_load(data)
        except YAMLError as exc:
            logging.info(f"yaml error: {exc}")
    roles = roles_yaml["roles"]

    return roles


def get_role_inheritances(role_name: str, roles_list: list) -> list:
        """
        Traverse list of dictionaries with snowflake roles to compile role inheritances
        """
        role_inheritances = next(
            (
                role[role_name].get("member_of", [])
                for role in roles_list
                if role.get(role_name)
            ),
            [],
        )
        return role_inheritances + [
            inherited
            for direct in role_inheritances
            for inherited in get_role_inheritances(direct, roles_list)
        ]



role = "rdemiri"
all_roles = get_roles()

inherited_roles = get_role_inheritances(role, all_roles)
logging.info(f"found inherited roles: {inherited_roles}")
inherited_roles_in = "', '".join(inherited_roles)
