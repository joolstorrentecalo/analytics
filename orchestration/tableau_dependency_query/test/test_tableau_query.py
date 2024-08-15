"""
This file contains tests for the tableau_query.py file.
"""

from orchestration.tableau_dependency_query.src.tableau_query import (
    query_table,
    get_table_path_query,
)


def test_get_table_path_query():
    """
    Test get_table_path_query function
    """
    table_name = "gitlab_db_ci_builds"
    expected_path = "raw:tap_postgres.gitlab_db_ci_builds"

    # Patch get_response with mock
    actual_path = get_table_path_query(table_name)

    assert actual_path == expected_path


def test_get_table_path_query_no_path():
    """
    Test get_table_path_query function when no path is returned
    """
    table_name = "missing_table"
    expected_path = None
    response = get_table_path_query(table_name)

    assert response == expected_path


def test_query_table():
    """
    Test query_table function
    """
    full_table_path = "raw:tap_zendesk.tickets"
    expected_mcon = "MCON++07a010bd-4365-442a-9bec-c06ac57c28dc++557c5de5-160a-4026-99fb-9ec2985c510b++table++raw:tap_zendesk.tickets"

    actual_mcon = query_table(full_table_path)

    assert actual_mcon == expected_mcon
