""" The main test routine for Level Up """

import os

import pytest
import requests
import responses

# can't import without key
os.environ["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"] = "some_key"
from extract.level_up_thought_industries.src.thought_industries_api import (
    CourseCompletions,
    Logins,
    Visits,
    CourseViews,
)

from extract.level_up_thought_industries.src.thought_industries_api_helpers import (
    iso8601_to_epoch_ts_ms,
    epoch_ts_ms_to_datetime_str,
    is_invalid_ms_timestamp,
)


def test_instantiation():
    """Test that all child classes can be instantiated"""
    class_name = "CourseCompletions"
    try:
        CourseCompletions()
    except TypeError as err:
        assert False, f"Failed to instantiate {class_name}: {str(err)}"

    class_name = "Logins"
    try:
        Logins()
    except TypeError as err:
        assert False, f"Failed to instantiate {class_name}: {str(err)}"

    class_name = "Visits"
    try:
        Visits()
    except TypeError as err:
        assert False, f"Failed to instantiate {class_name}: {str(err)}"

    class_name = "CourseViews"
    try:
        CourseViews()
    except TypeError as err:
        assert False, f"Failed to instantiate {class_name}: {str(err)}"


def test_iso8601_to_epoch_ts_ms():
    """Test that iso8601 date string is converted correctly"""
    # Test that func returns correct value
    valid_iso8601 = "2023-02-10T16:44:45.084Z"
    res = iso8601_to_epoch_ts_ms(valid_iso8601)
    assert res == 1676047485084

    # Test that ValueError is thrown if iso is invalid
    invalid_iso8601 = "2023-13-10T25:44:45.084Z"
    error_str = "month must"
    with pytest.raises(ValueError) as exc:
        res = iso8601_to_epoch_ts_ms(invalid_iso8601)
    assert error_str in str(exc.value)


def test_epoch_ts_ms_to_datetime_str():
    """Test that epoch ts is converted to datetime str"""
    epoch_ts_ms = 1675904400000
    res = epoch_ts_ms_to_datetime_str(epoch_ts_ms)
    assert res == "2023-02-09 01:00:00"


def test_is_invalid_ms_timestamp():
    """Test that valid/invalid timestamps are handled correctly"""

    valid_epoch_start_ms = 1104541200000  # 2005-01-01
    valid_epoch_end_ms = 4075049156000  # 2099-02-17

    # soley invalid in the eyes of custom function
    invalid_epoch_start_ms = 915152400000  # 1999-01-01
    invalid_epoch_end_ms = 32503683600000  # 3000-01-01

    # valid start / valid end = valid
    res = is_invalid_ms_timestamp(valid_epoch_start_ms, valid_epoch_end_ms)
    assert not res

    # invalid start / valid end = invalid
    res = is_invalid_ms_timestamp(invalid_epoch_start_ms, valid_epoch_end_ms)
    assert res

    # valid start / invalid end = invalid
    res = is_invalid_ms_timestamp(valid_epoch_start_ms, invalid_epoch_end_ms)
    assert res

    # invalid start / valid end = invalid
    res = is_invalid_ms_timestamp(invalid_epoch_start_ms, invalid_epoch_end_ms)
    assert res
