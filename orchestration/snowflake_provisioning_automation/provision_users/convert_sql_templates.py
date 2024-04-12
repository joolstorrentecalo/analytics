"""
This module does the following:
- Reads in sql file that is Jinja templated
- Renders the Jinja template with the Snowflake username
- Converts the sql file to a series of sql statements
"""

import os
import sqlparse
from jinja2 import Template

PARENT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_template(filename: str) -> str:
    """
    Return as Jinja Template based on filename of sql file
    """
    file_path = os.path.join(PARENT_DIR, "sql_templates/", filename)
    with open(file_path, "r", encoding="utf-8") as f:
        file_contents = f.read()

    # Create a Jinja2 Template object
    return file_contents


def convert_to_sql_statements(template_filename: str) -> list:
    """
    The sql file is read in as one string
    Using sqlparse lib, this function converts the string into a series of sql statements
    """
    sql_file_contents = get_template(template_filename)
    statements = sqlparse.split(sql_file_contents)
    return [str(statement) for statement in statements if statement.strip()]
