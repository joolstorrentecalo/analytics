"""
This module takes in a raw, templated string
renders it using Jinja, then reads file in as json
"""

import logging
import json
from typing import Union
from jinja2 import Template


def render_template_for_username(
    username: str, template_content: str
) -> Union[dict, list]:
    """
    Renders string template using Jinja templating
    Then reads in the string as json obj (dict/list)
    """
    prod_database = f"{username}_prod"
    prep_database = f"{username}_prep"
    context = {
        "username": username,
        "prod_database": prod_database,
        "prep_database": prep_database,
        "prod_schemas": f"{prod_database}.*",
        "prep_schemas": f"{prep_database}.*",
        "prod_tables": f"{prod_database}.*.*",
        "prep_tables": f"{prep_database}.*.*",
    }
    template = Template(template_content)
    rendered_template = template.render(**context)
    try:
        value = json.loads(rendered_template)
    except json.JSONDecodeError:
        logging.error(
            f"The provided rendered_template cannot be converted to valid JSON: {rendered_template}"
        )
        raise
    return value


def concat_template_values(usernames: list, template: str) -> list:
    """
    Each template is converted to a json dict
    And each json dict is stored in a values list
    These values will be added to roles.yml
    """
    first_iteration, is_list, is_dict = True, False, False
    values = []
    for username in usernames:
        value = render_template_for_username(username, template)
        if first_iteration:
            if isinstance(value, dict):
                is_dict = True
            elif isinstance(value, list):
                is_list = True
            else:
                raise ValueError(f"invalid template structure: {value}")
            first_iteration = False

        if is_dict:
            values.append(value)
        elif is_list:
            values = values + value

    return values
