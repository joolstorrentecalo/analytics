import os

import pytest
from dags.kubernetes_helpers import get_toleration, get_affinity


@pytest.fixture(autouse=True)
def run_around_tests():
    if "NAMESPACE" in os.environ:
        current_namespace = os.environ["NAMESPACE"]
    else:
        current_namespace = ""
    yield
    os.environ["NAMESPACE"] = current_namespace


def set_testing_env():
    os.environ["NAMESPACE"] = "testing"


def get_affinity_name_from_value(affinity):
    return affinity["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
        "nodeSelectorTerms"
    ][0]["matchExpressions"][0]["key"]


def test_affinity():
    extraction_highmem_affinity = get_affinity("extraction_highmem")
    dbt_affinity = get_affinity("dbt")

    set_testing_env()
    test_affinity = get_affinity("dbt")

    assert (
        get_affinity_name_from_value(extraction_highmem_affinity)
        == "extraction_highmem"
    )
    assert get_affinity_name_from_value(dbt_affinity) == "dbt"
    assert get_affinity_name_from_value(test_affinity) == "test"


def get_toleration_name_from_value(toleration):
    return toleration[0]["key"]


def test_toleration():
    extraction_highmem_toleration = get_toleration("extraction_highmem")
    dbt_toleration = get_toleration("dbt")
    data_science_toleration = get_toleration("data_science")

    set_testing_env()
    test_toleration = get_toleration("dbt")

    assert (
        get_toleration_name_from_value(extraction_highmem_toleration)
        == "extraction_highmem"
    )
    assert get_toleration_name_from_value(test_toleration) == "test"
    assert get_toleration_name_from_value(dbt_toleration) == "dbt"
    assert get_toleration_name_from_value(data_science_toleration) == "data_science"
