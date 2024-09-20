""" The main test routine for Level Up """

import os
import unittest
from unittest.mock import patch, Mock, MagicMock
import pytest
from sqlalchemy.engine.base import Engine

# can't import without key
os.environ["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"] = "some_key"
from thought_industries_api import (
    CourseCompletions,
    Users,
)
from thought_industries_api_helpers import epoch_ts_ms_to_datetime_str


class TestCursorEndpoint(unittest.TestCase):
    """Test suite for CursorEndpoint"""

    def setUp(self):
        """setup class"""
        self.user_endpoint = Users()

    def test_get_name(self):
        """test get_name()"""
        expected_result = "users"
        assert self.user_endpoint.get_name() == expected_result
        assert self.user_endpoint.name == expected_result

    def test_get_endpoint_url(self):
        """test get_endpoint_url()"""
        expected_result = "https://university.gitlab.com/incoming/v2/users"
        assert self.user_endpoint.get_endpoint_url() == expected_result
        assert self.user_endpoint.endpoint_url == expected_result

    def test_get_cursor_url(self):
        """test get_cursor_url()"""
        cursor = "someCursor"
        formatted_cursor = f"?cursor={cursor}"
        expected_result = f"{self.user_endpoint.endpoint_url}{formatted_cursor}"
        result = self.user_endpoint.get_cursor_url(cursor)
        assert result == expected_result

        cursor = ""
        result = self.user_endpoint.get_cursor_url(cursor)
        expected_result = self.user_endpoint.endpoint_url
        assert result == expected_result

    @patch("thought_industries_api.CursorEndpoint.read_cursor_state")
    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_has_more(
        self, mock_make_request, mock_read_cursor_state
    ):
        """
        Test that `fetch_from_endpoint` terminates on first response once hasMore=False
        Response is mocked so that hasMore=False on first response
        """
        mock_read_cursor_state.return_value = "some_cursor"
        users_value = [{"name": "John Doe"}]

        mock_response = Mock()
        mock_response.json.return_value = {
            "users": users_value,
            "pageInfo": {"hasMore": False},  # Termination condition
        }
        mock_make_request.return_value = mock_response

        metadata_engine = MagicMock(spec=Engine)
        results, _, has_more = self.user_endpoint.fetch_from_endpoint(metadata_engine)

        self.assertEqual(results, users_value)
        self.assertEqual(has_more, False)
        mock_make_request.assert_called_once()  # Only one call due to termination

    @patch("thought_industries_api.CursorEndpoint.read_cursor_state")
    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_record_threshold(
        self, mock_make_request, mock_read_cursor_state
    ):
        """
        Test that `fetch_from_endpoint` terminates after RECORD_THRESHOLD has been reached
        Response is mocked so that RECORD_THRESHOLD is reached by the 2nd response
        """
        mock_read_cursor_state.return_value = "some_cursor"
        # this multipler always ensures we'll reach the record threshold within 2 calls
        multiplier = int(self.user_endpoint.RECORD_THRESHOLD * 0.6)
        users_value = [{"name": "John Doe"}] * multiplier

        mock_response = Mock()
        mock_response.json.return_value = {
            "users": users_value,
            "pageInfo": {"hasMore": True, "cursor": "some_cursor"},
        }

        # Setting the mock to return two responses sequentially
        mock_make_request.side_effect = [mock_response, mock_response]

        metadata_engine = MagicMock(spec=Engine)
        results, _, has_more = self.user_endpoint.fetch_from_endpoint(metadata_engine)

        self.assertEqual(results, users_value * 2)
        self.assertEqual(has_more, True)
        self.assertEqual(
            mock_make_request.call_count, 2
        )  # 2 calls to reach RECORD_THRESHOLD


class TestDateIntervalEndpoint(unittest.TestCase):
    """Test suite for CursorEndpoint"""

    def setUp(self):
        """setup class"""
        self.course_endpoint = CourseCompletions()

    def test_get_name(self):
        """test get_name()"""
        expected_result = "course_completions"
        assert self.course_endpoint.get_name() == expected_result
        assert self.course_endpoint.name == expected_result

    def test_get_endpoint_url(self):
        """test get_endpoint_url()"""
        expected_result = (
            "https://university.gitlab.com/incoming/v2/events/courseCompletion"
        )
        assert self.course_endpoint.get_endpoint_url() == expected_result
        assert self.course_endpoint.endpoint_url == expected_result

    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_empty_results(self, mock_make_request):
        """
        Test that `fetch_from_endpoint` terminates if `results` is empty
        events_value is mocked to [] to reproduce this
        """
        epoch_start_ms = 1722384000001
        epoch_end_ms = 1722470400000
        events_value = []

        mock_response = Mock()
        mock_response.json.return_value = {
            "events": events_value,
        }
        mock_make_request.return_value = mock_response
        results, _ = self.course_endpoint.fetch_from_endpoint(
            epoch_start_ms, epoch_end_ms
        )

        self.assertEqual(results, events_value)
        mock_make_request.assert_called_once()  # Only one call due to termination

    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_record_threshold(
        self, mock_make_request
    ):
        """
        Test that `fetch_from_endpoint` terminates once record threshold is reached
        The record threshold should be reached in 2 calls based off the `multipler` logic

        To set-up, the following needs to be done:
        - mock the first response, which will be a later record (since endpoint returns in reverse)
        -mock the 2nd responsem, which will be an eariler record

        In addition, test the following:
            - the function returns the combined event results of the two responses
            - the current_epch_ts is the last returned epoch_ts-1

        """
        epoch_start_ms = 1722384000001
        epoch_end_ms = 1722470400000

        some_ts = epoch_end_ms
        some_datetime_str = epoch_ts_ms_to_datetime_str(some_ts)
        some_earlier_ts = epoch_end_ms - 3600000
        some_earlier_datetime_str = epoch_ts_ms_to_datetime_str(some_earlier_ts)
        # this multipler always ensures we'll reach the record threshold within 2 calls
        multiplier = int(self.course_endpoint.RECORD_THRESHOLD * 0.6)

        # mock response 1 (later timestamp)
        mock_response1 = Mock()
        events_value1 = [
            {"id": "some_id", "timestamp": some_datetime_str},
        ] * multiplier
        mock_response1.json.return_value = {"events": events_value1}

        # mock response 2 (earlier timestamp)
        mock_response2 = Mock()
        events_value2 = [
            {"id": "some_earlier_id", "timestamp": some_earlier_datetime_str},
        ] * multiplier
        mock_response2.json.return_value = {"events": events_value2}

        # Setting the mock to return two responses sequentially
        mock_make_request.side_effect = [mock_response1, mock_response2]

        results, current_epoch_end_ms = self.course_endpoint.fetch_from_endpoint(
            epoch_start_ms, epoch_end_ms
        )

        self.assertEqual(results, events_value1 + events_value2)
        self.assertEqual(current_epoch_end_ms, some_earlier_ts - 1)
        self.assertEqual(
            mock_make_request.call_count, 2
        )  # 2 calls to reach RECORD_THRESHOLD

    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_value_error(self, mock_make_request):
        """
        Test that `fetch_from_endpoint` raises ValueError,
        if the epoch_end_ms does not change.

        This is reproduced by returning the same `events_value` each requst
        """
        epoch_start_ms = 1722384000001
        epoch_end_ms = 1722470400000

        some_ts = epoch_end_ms
        some_datetime_str = epoch_ts_ms_to_datetime_str(some_ts)

        mock_response = Mock()
        events_value = [
            {"id": "some_id", "timestamp": some_datetime_str},
        ]
        mock_response.json.return_value = {"events": events_value}
        mock_make_request.return_value = mock_response

        error_str = "endDate parameter has not changed"
        with pytest.raises(ValueError) as exc:
            _, _ = self.course_endpoint.fetch_from_endpoint(
                epoch_start_ms, epoch_end_ms
            )
        assert error_str in str(exc.value)
