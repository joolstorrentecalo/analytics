"""
The main test routine for utils for Automated Service Ping
"""

import os
import re
from datetime import datetime
from unittest import mock

import pytest
import requests
import responses
from extract.saas_usage_ping.utils import EngineFactory, Utils


@pytest.fixture(autouse=True, name="engine_factory")
def create_engine_factory():
    """
    Create class object
    """
    return EngineFactory()


@pytest.fixture(autouse=True, name="set_env_variables")
def mock_settings_env_vars():
    """
    Simulate OS env. variables
    """
    with mock.patch.dict(
        os.environ,
        {"GITLAB_ANALYTICS_PRIVATE_TOKEN": "xxx"},
    ):
        yield


@pytest.fixture(autouse=True, name="utils")
def create_utils(set_env_variables):
    """
    Create class object
    """
    _ = set_env_variables

    return Utils()


@pytest.fixture(name="fake_response")
def mocked_responses():
    """
    Mock routine to create fake response
    """
    with responses.RequestsMock() as rsps:
        yield rsps


def test_static_variables(utils):
    """
    Test case: check static variables
    """

    assert (
        utils.TRANSFORMED_INSTANCE_SQL_QUERIES_FILE
        == "transformed_instance_queries.json"
    )
    assert (
        utils.META_DATA_INSTANCE_SQL_QUERIES_FILE == "meta_data_instance_queries.json"
    )
    assert utils.NAMESPACE_FILE == "usage_ping_namespace_queries.json"
    assert utils.HAVING_CLAUSE_PATTERN == re.compile(
        "HAVING.*COUNT.*APPROVAL_PROJECT_RULES_USERS.*APPROVALS_REQUIRED", re.IGNORECASE
    )
    assert utils.ENCODING == "utf8"
    assert utils.SQL_KEY == "sql"
    assert utils.REDIS_KEY == "redis"


def test_excluded_sql_metrics(utils):
    """
    Test content of excluded metrics
    """
    actual = (
        1 if (x.startswith("usage_activity_by_stage") or x.startswith("counts")) else 0
        for x in utils.METRICS_EXCEPTION_INSTANCE_SQL
    )
    assert all(actual)


def test_engine_factory(engine_factory):
    """
    Test Class creation
    """
    assert engine_factory is not None


def test_engine_factory_processing_warehouse(engine_factory):
    """
    Test Class properties - processing_role
    """
    assert engine_factory.processing_role == "LOADER"


def test_engine_factory_schema_name(engine_factory):
    """
    Test Class properties - schema_name
    """
    assert engine_factory.schema_name == "saas_usage_ping"


def test_engine_factory_loader_engine(engine_factory):
    """
    Test Class properties - loader_engine
    """
    assert engine_factory.loader_engine is None


def test_engine_factory_config_vars(engine_factory):
    """
    Test Class properties - config_vars
    """
    assert engine_factory.config_vars is not None


def test_engine_factory_connected(engine_factory):
    """
    Test Class properties - connected
    """
    assert engine_factory.connected is False


def test_utils(utils):
    """
    Test Class creation
    """
    assert utils is not None


def test_headers(utils):
    """
    Test Class properties - headers
    """
    assert utils.headers["PRIVATE-TOKEN"] == "xxx"


def test_headers_error(utils):
    """
    Test Class properties - headers
    """
    with pytest.raises(KeyError):
        assert utils.headers["WRONG_KEY"] == "xxx"


def test_convert_response_to_json(utils, fake_response):
    """
    Test function: convert_response_to_json
    """
    expected = {"test1": "pro", "test2": "1"}
    fake_response.get(
        "http://some_gitlab_api_url/test",
        body='{"test1": "pro", "test2": "1"}',
        status=200,
        content_type="application/json",
    )

    resp = requests.get("http://some_gitlab_api_url/test")

    actual = utils.convert_response_to_json(response=resp)

    assert actual == expected


def test_get_response(utils):
    """
    Force fake url and raise a Connection Error
    """
    with pytest.raises(ConnectionError):
        _ = utils.get_response("http://fake_url/test")


def test_keep_meta_data(utils):
    """
    Test routine keep_meta_data
    """

    fake_json = {
        "not_in_meta": "1",
        "recorded_at": "1",
        "version": "1",
        "edition": "1",
        "recording_ce_finished_at": "1",
        "recording_ee_finished_at": "1",
        "uuid": "1",
        "not_in_meta2": "1",
    }

    meta_data = utils.keep_meta_data(fake_json)
    actual = list(meta_data.keys())

    assert isinstance(meta_data, dict)
    assert utils.META_API_COLUMNS == actual
    assert "not_in_meta" not in actual
    assert "not_in_meta2" not in actual
    assert "recorded_at" in actual


def test_get_loaded_metadata(utils):
    """
    Test rotuine get_loaded_metadata
    """

    expected = {"SQL": {"test": "1"}, "Redis": {"test2": "2"}}

    actual = utils.get_loaded_metadata(
        keys=["SQL", "Redis"], values=[{"test": "1"}, {"test2": "2"}]
    )

    assert actual == expected


def test_get_loaded_metadata_empty(utils):
    """
    Test rotuine get_loaded_metadata empty list(s)
    """

    expected = {}

    actual = utils.get_loaded_metadata(keys=[], values=[])

    assert actual == expected


def test_get_md5(utils):
    """
    Simple MD5 test.
    Know testing the private method is not aligned
    with the best practices,
    but found it is sufficient
    in this implementation.
    """

    input_timestamps = [
        datetime(2021, 9, 1, 23, 10, 21).timestamp(),
        datetime(2020, 8, 1, 23, 10, 22).timestamp(),
        datetime(2021, 7, 1, 23, 10, 23).timestamp(),
        "test_string",
        "",
        None,
    ]

    for check_time in input_timestamps:
        res = utils.get_md5(check_time)

        # Check output data type
        assert isinstance(res, str)
        # Check is len 32 as it is expected length
        assert len(res) == 32  # bytes in hex representation
        # As this is one-way function,
        # can't test it with many things
        # let see to we have all details with various inputs
        assert res is not None


@pytest.mark.parametrize(
    "test_value, expected_value",
    [("non_mapped_table", "non_mapped_table"), ("p_ci_builds", "ci_builds")],
)
def test_get_metric_table_name(utils, test_value, expected_value):
    """
    Test mapping dict for tables
    """
    assert utils.get_metric_table_name(test_value) == expected_value
