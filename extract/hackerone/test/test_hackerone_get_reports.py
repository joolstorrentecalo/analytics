"""
Test for hackerone_get_reports.py
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
from hackerone_get_reports import (
    get_start_and_end_date,
    get_reports,
)


class TestHackerOneGetReports(unittest.TestCase):
    @patch("hackerone_get_reports.IS_FULL_REFRESH", False)
    def test_get_start_and_end_date_not_full_refresh(self):
        start_date, end_date = get_start_and_end_date()
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(start_date, yesterday)
        self.assertEqual(end_date, today)

    @patch("hackerone_get_reports.IS_FULL_REFRESH", True)
    def test_get_start_and_end_date_full_refresh(self):
        start_date, end_date = get_start_and_end_date()
        self.assertEqual(start_date, "2020-01-01T00:00:00Z")
        self.assertTrue(end_date.startswith(datetime.now().strftime("%Y-%m-%d")))

    @patch("hackerone_get_reports.requests.get")
    def test_get_reports(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "123",
                    "attributes": {
                        "state": "triaged",
                        "created_at": "2023-01-01T00:00:00Z",
                    },
                    "relationships": {"bounties": {}},
                }
            ],
            "links": {},
        }
        mock_get.return_value = mock_response

        start_date = "2023-01-01T00:00:00Z"
        end_date = "2023-01-02T00:00:00Z"
        result = get_reports(start_date, end_date)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["id"], "123")
        self.assertEqual(result.iloc[0]["state"], "triaged")
        self.assertEqual(result.iloc[0]["created_at"], "2023-01-01T00:00:00Z")
