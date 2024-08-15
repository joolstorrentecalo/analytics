"""
The main test routine for instance_namespace_metrics
"""

from datetime import datetime, timedelta

import pytest

from extract.saas_usage_ping.instance_namespace_metrics import InstanceNamespaceMetrics


@pytest.fixture(name="namespace_ping")
def get_namespace_ping():
    """
    Return NamespacePing object
    """
    namespace_ping = InstanceNamespaceMetrics()
    namespace_ping.end_date = datetime.now()
    namespace_ping.start_date_28 = namespace_ping.end_date - timedelta(days=28)

    return namespace_ping


@pytest.fixture(name="namespace_file")
def get_usage_ping_namespace_file(namespace_ping):
    """
    Fixture for namespace file
    """

    return namespace_ping.get_meta_data_from_file(
        file_name="usage_ping_namespace_queries.json"
    )


def test_static_variables(namespace_ping):
    """
    Check static variables
    """
    assert namespace_ping.utils.NAMESPACE_FILE == "usage_ping_namespace_queries.json"


def test_namespace_ping_class(namespace_ping):
    """
    Check class creation
    """
    assert namespace_ping


@pytest.mark.parametrize(
    "test_value, expected_value",
    [
        ("active_user_count", False),
        (
            "usage_activity_by_stage_monthly.manage.groups_with_event_streaming_destinations",
            True,
        ),
        ("usage_activity_by_stage_monthly.manage.audit_event_destinations", True),
        ("counts.boards", False),
        ("usage_activity_by_stage_monthly.configure.instance_clusters_enabled", True),
        ("counts_monthly.deployments", True),
    ],
)
def test_get_backfill_filter(
    namespace_ping, namespace_file, test_value, expected_value
):
    """
    test backfill filter accuracy with
    lambda as a return statement
    """

    metrics_filter = namespace_ping.filter_instance_namespace_metrics([test_value])

    for namespace in namespace_file:
        if metrics_filter(namespace):
            assert namespace.get("time_window_query") == expected_value
            assert expected_value is True
            assert namespace.get("counter_name") == test_value


def test_json_file_consistency_time_window_query(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If time_window_query=True,
    counter_query should contain ["between_start_date","between_end_date"]
    """

    for metrics in namespace_file:
        counter_query = metrics.get("counter_query")
        time_window_query = bool(metrics.get("time_window_query", False))

        time_window_yes = (
            "between_start_date" in counter_query
            and "between_end_date" in counter_query
            and time_window_query is True
        )
        time_window_no = (
            "between_start_date" not in counter_query
            and "between_end_date" not in counter_query
            and time_window_query is False
        )

        assert time_window_yes or time_window_no


def test_namespace_file(namespace_file):
    """
    Test file loading
    """

    assert namespace_file


def test_namespace_file_error(namespace_ping):
    """
    Test file loading
    """
    with pytest.raises(FileNotFoundError):
        namespace_ping.get_meta_data_from_file(file_name="THIS_DOES_NOT_EXITS.json")


def test_json_file_consistency_level(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If level=namespace
    """

    for metrics in namespace_file:
        level = metrics.get("level")

        assert level == "namespace"


def test_replace_placeholders(namespace_ping):
    """
    Test string replace for query
    """
    sql = "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"

    actual = namespace_ping.replace_placeholders(sql=sql)

    assert "between_start_date" not in actual
    assert "between_end_date" not in actual

    assert datetime.strftime(namespace_ping.end_date, "%Y-%m-%d") in actual
    assert datetime.strftime(namespace_ping.start_date_28, "%Y-%m-%d") in actual


def test_validate_namespace_queries(namespace_file):
    """
    Test does namespace file have proper SQL
    % -> %%
    """

    for n in namespace_file:
        if "%" in n.get("counter_query", ""):
            assert "%%" in n.get("counter_query", "")


def test_prepare_insert_query(namespace_file, namespace_ping):
    """
    Test query replacement
    """
    insert_part = (
        "(id, namespace_ultimate_parent_id, counter_value, "
        "ping_name, level, query_ran, error, ping_date, _uploaded_at)"
    )
    for namespace_query in namespace_file:
        expected = namespace_ping.prepare_insert_query(
            query_dict=namespace_query.get("counter_query"),
            ping_name=namespace_query.get("counter_name"),
            ping_date=namespace_ping.end_date,
        )
        assert insert_part in expected
        assert "Success" in expected
        assert "namespace" in expected
        assert "FROM" in expected
        assert expected.count("INSERT") == 1


@pytest.mark.parametrize(
    "test, expected",
    [
        ((1, 20), (0, 30)),
        ((2, 20), (30, 60)),
        ((3, 20), (60, 90)),
        ((20, 20), (570, 600)),
        ((1, 19), (0, 31)),
        ((2, 19), (31, 62)),
    ],
)
def test_chunk_list(namespace_file, namespace_ping, test, expected):
    """
    Test list slicing
    for dynamic queries splitting
    """
    namespace_ping.chunk_no = test[0]
    namespace_ping.number_of_tasks = test[1]

    actual = namespace_ping.chunk_list(namespace_size=582)

    assert actual == expected


def test_chunk_list_edge_case(namespace_file, namespace_ping):
    """
    Test list slicing
    for dynamic queries splitting
    """
    namespace_ping.chunk_no = 0
    namespace_ping.number_of_tasks = 0

    actual = namespace_ping.chunk_list(namespace_size=len(namespace_file))

    assert actual == (0, len(namespace_file))


def test_generate_error_message(namespace_ping):
    """
    Test generate_error_message
    """

    error_message = """(snowflake.connector.errors.ProgrammingError) 001003 (42000): 01ae51f0-0406-8c1e-0000-289d5a74c882: SQL compilation error:\n'
[2023-08-15 08:16:59,539] INFO - b"syntax error line 1 at position 343 unexpected '{'.\n
[2023-08-15 08:16:59,539] INFO - b"syntax error line 1 at position 342 unexpected '.'.\n"""

    expected = namespace_ping.generate_error_message(input_error=error_message)

    assert "\n" not in expected
    assert "'" not in expected


def test_generate_error_message_sql(namespace_ping):
    """
    Test generate_error_message
    """

    error_message = "(snowflake.connector.errors.ProgrammingError) 001003 (42000): 01ae51f0-0406-8c1e-0000-289d5a74c882  This is remaining part[SQL: This will be deleted"

    expected = namespace_ping.generate_error_message(input_error=error_message)

    assert "[SQL:" not in expected
    assert "This will be deleted" not in expected
    assert "This is remaining part" in expected


def test_generate_error_insert(namespace_ping):
    """
    Test generate_error_insert
    """
    metrics_sql_select = "SELECT 1"
    expected = namespace_ping.generate_error_insert(
        metrics_name="test_metrics",
        metrics_level="namespace",
        metric_sql_select=metrics_sql_select,
        error_text="Test error message",
    )

    assert namespace_ping.SQL_INSERT_PART in expected
    assert metrics_sql_select in expected
    assert "namespace" in expected
    assert "NULL, NULL, NULL" in expected
    assert "\\" not in expected


def test_multiple_select_statement(namespace_file, namespace_ping):
    """
    Test option when we have 2 SELECT statement (subquery),
    as it was failing before.
    Example:
    Metrics: counts.service_desk_issues
    SQL (have 2 SELECT statement):
        SELECT
          namespaces_xf.namespace_ultimate_parent_id  AS id,
          namespaces_xf.namespace_ultimate_parent_id,
          COUNT(issues.id)                            AS counter_value
        FROM prep.gitlab_dotcom.gitlab_dotcom_issues_dedupe_source AS issues
        LEFT JOIN prep.gitlab_dotcom.gitlab_dotcom_projects_dedupe_source AS projects
          ON projects.id = issues.project_id
        LEFT JOIN prep.gitlab_dotcom.gitlab_dotcom_namespaces_dedupe_source AS namespaces
          ON namespaces.id = projects.namespace_id
        LEFT JOIN prod.legacy.gitlab_dotcom_namespaces_xf AS namespaces_xf
          ON namespaces.id = namespaces_xf.namespace_id
        WHERE issues.project_id IN
          (
            SELECT projects.id
            FROM prep.gitlab_dotcom.gitlab_dotcom_projects_dedupe_source AS projects
            WHERE projects.service_desk_enabled = TRUE
          )
            AND issues.author_id = 1257257
            AND issues.confidential = TRUE
        GROUP BY 1

    """
    for metrics in namespace_file:
        actual = namespace_ping.prepare_insert_query(
            query_dict=metrics.get("counter_query"),
            ping_name=metrics.get("counter_name"),
            ping_date=namespace_ping.end_date,
        )

        assert actual.count("INSERT INTO") == 1
        assert actual.count("DATE_PART(epoch_second, CURRENT_TIMESTAMP())") == 1
        assert actual.count("Success") == 1
        assert actual.count(metrics.get("counter_name")) == 1
