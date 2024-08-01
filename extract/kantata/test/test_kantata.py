""" Test kantata.py module """

import os
import unittest
from unittest.mock import patch

# set env prior to importing kantata.config_dict
os.environ["data_interval_start"] = "2024-01-01T08:00:00"
os.environ["data_interval_end"] = "2024-01-02T08:00:00"
from kantata import (
    download_report_from_s3,
    has_valid_latest_export,
    retrieve_insight_report_external_identifier,
    retrieve_scheduled_insight_report,
)


def test_retrieve_insight_report_external_identifier():
    """
    Test1: Test that the correct identifier is returned
    Test2: Test that None is returned when the report title doesn't exist
    Test3: Test that when there are multiple reports with the same matching title,
    that the identifier with the most recent created_at is returned
    """
    report_name_to_find = "some_report"

    # Test1
    insight_reports = [
        {
            "title": "some_report",
            "identifier": "some_identifier",
            "created_at": "1999-12-31",
        },
        {
            "title": "some_other_report",
            "identifier": "some_other_identifier",
            "created_at": "2024-01-01",
        },
    ]
    identifier = retrieve_insight_report_external_identifier(
        report_name_to_find, insight_reports
    )
    assert identifier == "some_identifier"

    # Test2
    insight_reports = [{}]
    identifier = retrieve_insight_report_external_identifier(
        report_name_to_find, insight_reports
    )
    assert identifier is None

    # Test3
    insight_reports = [
        {
            "title": "same_title",
            "identifier": "correct_identifier",
            "created_at": "2024-01-01",
        },
        {
            "title": "same_title",
            "identifier": "wrong_identifier",
            "created_at": "1999-12-31",
        },
        {
            "title": "other_title",
            "identifier": "some_identifier",
            "created_at": "2024-01-01",
        },
    ]
    report_name_to_find = "same_title"
    identifier = retrieve_insight_report_external_identifier(
        report_name_to_find, insight_reports
    )
    assert identifier == "correct_identifier"


def test_retrieve_scheduled_insight_report():
    """
    Test1: Test that the correct report dict is returned
    Test2: Test that None is returned when the identifier doesn't exist
    Test3: Test that the identifier with the most recent created_at is returned
    """
    report_d1 = {"created_at": "1999-12-31"}
    report_d2 = {
        "external_report_object_identifier": "some_identifier",
        "title": "some_title",
        "created_at": "2024-01-01",
    }
    scheduled_insight_reports = [report_d1, report_d2]

    # Test1
    report_external_identifier = "some_identifier"
    result_d = retrieve_scheduled_insight_report(
        report_external_identifier, scheduled_insight_reports
    )
    assert result_d == report_d2

    # Test2
    report_external_identifier = "some_missing_identifier"
    result_d = retrieve_scheduled_insight_report(
        report_external_identifier, scheduled_insight_reports
    )
    assert result_d == {}

    # Test3
    report_d1 = {
        "external_report_object_identifier": "same_identifier",
        "title": "correct_dict",
        "created_at": "2024-01-02",
    }
    report_d2 = {
        "external_report_object_identifier": "same_identifier",
        "title": "wrong_dict",
        "created_at": "2024-01-01",
    }
    report_d3 = {
        "external_report_object_identifier": "some_identifier",
        "title": "some_title",
        "created_at": "2024-01-02",
    }
    scheduled_insight_reports = [report_d1, report_d2, report_d3]

    report_external_identifier = "same_identifier"
    result_d = retrieve_scheduled_insight_report(
        report_external_identifier, scheduled_insight_reports
    )
    assert result_d == report_d1


@patch("kantata.convert_timezone")
def test_has_valid_latest_export(mock_convert_timezone):
    """
    Test1: latest_result key doesn't exist
    Test2: latest_result['status'] == 'failure'
    Test3: latest_result['status'] == 'success' | created_at is outside the date_interval
    Test4: latest_result['status'] == 'success' | created_at == data_interval_end
    Test5: latest_result['status'] == 'success' | created_at is inside the date_interval

    Note: for the data_interval tests, the data_interval_start/end
    env variables are set at the top of this file, prior to imports
    """

    # Test1
    scheduled_insight_report = {
        "title": "Test1",
        "external_report_object_identifier": "some_identifier",
    }
    with unittest.TestCase().assertRaises(ValueError):
        has_valid_latest_export(scheduled_insight_report)

    # Test2
    scheduled_insight_report = {
        "title": "Test2",
        "latest_result": {"status": "failure"},
        "external_report_object_identifier": "some_identifier",
    }
    with unittest.TestCase().assertRaises(ValueError):
        has_valid_latest_export(scheduled_insight_report)

    # Test3
    scheduled_insight_report = {
        "title": "Test3",
        "latest_result": {"status": "success"},
        "external_report_object_identifier": "some_identifier",
        "created_at": "some_data_that_will_be_mocked",
    }

    # is 1 second before data_interval_start, invalid
    mock_convert_timezone.return_value = "2024-01-01T07:59:59"
    is_valid_latest_export3 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export3 is False

    # Test4
    # is the same as data_interval_end, invalid
    mock_convert_timezone.return_value = "2024-01-02T08:00:00"
    is_valid_latest_export4 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export4 is False

    # Test5
    scheduled_insight_report = {
        "title": "Test5",
        "latest_result": {"status": "success"},
        "external_report_object_identifier": "some_identifier",
        "created_at": "some_data_that_will_be_mocked",
    }
    # is the same as data_interval_start, valid
    mock_convert_timezone.return_value = "2024-01-01T08:00:00"
    is_valid_latest_export5 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export5 is True


@patch("kantata.make_request")
def test_download_report_from_s3(mock_make_request):
    """Test request was made correctly"""
    mock_response = mock_make_request.return_value
    mock_response.status_code = -1
    url = "some_url.com"
    latest_export = {"url": url}
    result = download_report_from_s3(latest_export)
    assert result.status_code == -1
    mock_make_request.assert_called_once_with("GET", url)
