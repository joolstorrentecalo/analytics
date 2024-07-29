""" Test kantata_utils.py """

from unittest.mock import MagicMock, patch

import pandas as pd
from kantata_utils import (
    add_csv_file_extension,
    clean_string,
    convert_timezone,
    have_columns_changed,
)
from pytest import raises


def test_convert_timezone():
    """
    Test1: make sure regular date is correctly converted
    Test2: Daylight savings time check
    Test2: check that correct error is thrown on invalid dt_pst_str
    """
    # Test1
    dt_pst_str = "2022-07-01T09:00:48"
    res = convert_timezone(dt_pst_str)
    assert res == "2022-07-01T16:00:48+00:00"

    # Test2
    dt_pst_str = "2022-01-01T08:00:48"
    res = convert_timezone(dt_pst_str)
    assert res == "2022-01-01T16:00:48+00:00"

    # Test3
    dt_pst_str = ""
    with raises(
        ValueError,
        match="Invalid isoformat string",
    ):
        convert_timezone(dt_pst_str)


def test_cleaned_string():
    """
    Test1: test a real Kantata report
    Test1.5: test another real Kantata report
    Test2: Make sure special characters are removed
    Test3: Make sure extra spaces are removed
    """
    # Test1
    str_to_clean = "Verify - Time Entry - Financial"
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "verify_time_entry_financial"

    # Test1.5
    str_to_clean = "API Download of ! Rev QBR: Details by Project & User"
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "rev_qbr_details_by_project_user"

    # Test2
    str_to_clean = "~some_&report)"
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "some_report"

    # Test3
    str_to_clean = "_some_ _other5     report "
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "some_other5_report"


def test_add_csv_file_extension():
    """Test file extension was added corrrectly"""
    prefix = "some_file"
    filename = add_csv_file_extension(prefix)
    assert filename == f"{prefix}.csv.gz"


@patch("kantata_utils.read_sql")
def test_have_columns_changed(mock_snowflake_read_sql):
    """
    Test1: The API and snowflake table has matching columns
    Test2: The API and snowflake table do NOT have matching columns
    """
    snowflake_engine = MagicMock()
    snowflake_table_name = "some_table"
    # Test1
    data = {
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
        "age": [28, 34],
    }
    mock_snowflake_read_sql.return_value = pd.DataFrame(data)
    mock_snowflake_read_sql.return_value["uploaded_at"] = ""

    df_api = pd.DataFrame(data)
    assert have_columns_changed(snowflake_engine, df_api, snowflake_table_name) is False

    # Test2
    data2 = {
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
    }
    df_api2 = pd.DataFrame(data2)
    assert have_columns_changed(snowflake_engine, df_api2, snowflake_table_name) is True
