"""
Test for hackerone_get_reports.py
"""

import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd

os.environ["is_full_refresh"] = "False"
os.environ["date_interval_start"] = "2022-01-01T08:00:00Z"
os.environ["date_interval_end"] = "2022-01-02T08:00:00Z"
from hackerone_get_reports import (
    get_start_and_end_date,
    get_reports,
)


class TestHackerOneGetReports(unittest.TestCase):
    """Test Class for hackerone_get_reports.py"""

    @patch("hackerone_get_reports.is_full_refresh", "False")
    def test_get_start_and_end_date_not_full_refresh(self):
        """Test get_start_and_end_date() when is_full_refresh is False"""
        start_date, end_date = get_start_and_end_date()
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT08:00:00Z")
        today = datetime.now().strftime("%Y-%m-%dT08:00:00Z")
        self.assertEqual(start_date, yesterday)
        self.assertEqual(end_date, today)

    @patch("hackerone_get_reports.is_full_refresh", "True")
    def test_get_start_and_end_date_full_refresh(self):
        """Test get_start_and_end_date() when is_full_refresh is True"""
        start_date, end_date = get_start_and_end_date()
        self.assertEqual(start_date, "2020-01-01T08:00:00Z")
        self.assertTrue(
            end_date.startswith(datetime.now().strftime("%Y-%m-%dT08:00:00Z"))
        )

    @patch("hackerone_get_reports.requests.get")
    def test_get_reports(self, mock_get):
        """Test get_reports()"""
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

    @patch(
        "hackerone_get_reports.nullify_vulnerability_information",
        return_value=pd.DataFrame(),
    )
    def test_nullify_vulnerability_information(self, return_df):
        """Check dataframe result"""
        return_df = pd.DataFrame()
        return_df["bounties"]["data"][0]["relationships"]["report"]["data"][
            "attributes"
        ]["vulnerability_information"] = None

        self.assertIsInstance(return_df, pd.DataFrame)
        # assert equal vulnerability information
        self.assertIsNone(
            return_df.iloc[0]["bounties"]["data"][0]["relationships"]["report"]["data"][
                "attributes"
            ]["vulnerability_information"]
        )
