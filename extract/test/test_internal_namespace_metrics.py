"""
Test internal namespace metrics
"""
from unittest.mock import patch

import pytest

from extract.saas_usage_ping.internal_namespace_metrics import (
    InternalNamespaceMetrics,
    SQLGenerate,
)


@pytest.fixture(name="internal_namespace_metrics")
def get_internal_namespace_metrics():
    """Fixture to create an instance of InternalNamespaceMetrics"""
    return InternalNamespaceMetrics()


@pytest.fixture(name="sql_generate")
def get_sql_generate():
    """Fixture to create an instance of SQLGenerate"""
    return SQLGenerate()


def test_sql_generate_get_time_frame(sql_generate):
    """Test the get_time_frame method of SQLGenerate class"""
    assert (
        sql_generate.get_time_frame("7d")
        == "AND behavior_at BETWEEN DATEADD(DAY, -7, between_end_date) AND between_end_date "
    )
    assert (
        sql_generate.get_time_frame("28d")
        == "AND behavior_at BETWEEN DATEADD(DAY, -28, between_end_date) AND between_end_date "
    )
    assert sql_generate.get_time_frame("1d") == ""


def test_sql_generate_get_event_list(sql_generate):
    """Test the get_event_list method of SQLGenerate class"""
    metrics = {
        "events": [{"name": "event1"}, {"name": "event2"}],
        "options": ["option1", "option2"],
    }
    expected = "AND redis_event_name IN ('event1','event2','option1','option2') "
    assert sql_generate.get_event_list(metrics) == expected

    metrics_single = {"events": [{"name": "single_event"}]}
    expected_single = "AND redis_event_name = 'single_event' "
    assert sql_generate.get_event_list(metrics_single) == expected_single


def test_sql_generate_transform(sql_generate):
    """Test the transform method of SQLGenerate class"""
    metrics = {
        "key_path": "***********",
        "events": [{"name": "event1"}],
        "time_frame": "7d",
    }
    expected = (
        "SELECT ultimate_parent_namespace_id AS id, "
        "ultimate_parent_namespace_id AS namespace_ultimate_parent_id, "
        "COUNT(DISTINCT gsc_pseudonymized_user_id) AS counter_value "
        "FROM prod.common_mart_product.mart_behavior_structured_event_service_ping_metrics "
        "WHERE metrics_path='***********' "
        "AND redis_event_name = 'event1' "
        "AND behavior_at BETWEEN DATEADD(DAY, -7, between_end_date) AND between_end_date "
        "GROUP BY ALL;"
    )
    assert sql_generate.transform(metrics) == expected


def test_internal_namespace_metrics_get_sql_metrics_definition(
    internal_namespace_metrics,
):
    """Test the get_sql_metrics_definition method of InternalNamespaceMetrics class"""
    metric = {
        "key_path": "test.metric",
        "events": [{"name": "event1"}],
        "time_frame": "7d",
    }
    result = internal_namespace_metrics.get_sql_metrics_definition(metric)
    assert result["counter_name"] == "test.metric"
    assert "SELECT" in result["counter_query"]
    assert result["time_window_query"] is True
    assert result["level"] == "namespace"


@patch("extract.saas_usage_ping.internal_namespace_metrics.Utils")
def test_internal_namespace_metrics_generate_sql_metrics(
    mock_utils, internal_namespace_metrics
):
    """Test the generate_sql_metrics method of InternalNamespaceMetrics class"""
    metrics_data = [
        {"key_path": "metric1", "status": "active"},
        {"key_path": "metric2", "status": "inactive"},
        {"key_path": "metric3", "status": "active"},
    ]
    result = internal_namespace_metrics.generate_sql_metrics(metrics_data)
    assert len(result) == 2
    assert result[0]["counter_name"] == "metric1"
    assert result[1]["counter_name"] == "metric3"


def test_internal_namespace_metrics_transform_yml_json(internal_namespace_metrics):
    """Test the transform_yml_json method of InternalNamespaceMetrics class"""
    yml_data = {"key": "value"}
    with patch.object(internal_namespace_metrics.util, "save_to_yml_file") as mock_save:
        with patch.object(
            internal_namespace_metrics.util, "load_from_yml_file"
        ) as mock_load:
            with patch.object(
                internal_namespace_metrics.util, "delete_file"
            ) as mock_delete:
                mock_load.return_value = yml_data
                result = internal_namespace_metrics.transform_yml_json(
                    "test.yml", yml_data
                )
                mock_save.assert_called_once_with(file_name="test.yml", data=yml_data)
                mock_load.assert_called_once_with(file_name="test.yml")
                mock_delete.assert_called_once_with(file_name="test.yml")
                assert result == yml_data
