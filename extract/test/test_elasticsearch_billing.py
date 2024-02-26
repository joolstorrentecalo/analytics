"""
The main test unit for ElasticSearch billing data
"""
import os
import pandas as pd

os.environ["ELASTIC_SEARCH_BILLING_API_KEY"] = "111"
os.environ["ELASTIC_CLOUD_ORG_ID"] = "111"

from extract.elasticsearch_billing.src.utility import HEADERS, prep_dataframe


def test_static_variables():
    """
    Test static variables
    """
    assert HEADERS["Content-Type"]
    assert HEADERS["Authorization"]


def test_prep_dataframe():
    """
    Test DataFrame from preparation
    """
    data = [[1, 2, 3], [4, 5, 6]]
    columns = ["a", "b", "c"]
    actual = prep_dataframe(data, columns)

    assert actual.shape == (2, 3)
    assert actual.columns[0] == "a"
    assert actual.columns[1] == "b"
    assert actual.columns[2] == "c"
    assert isinstance(actual, pd.DataFrame)
