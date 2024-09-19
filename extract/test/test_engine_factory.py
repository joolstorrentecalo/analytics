"""
Test engine_factory
"""
import pytest
from extract.saas_usage_ping.engine_factory import EngineFactory


@pytest.fixture(autouse=True, name="engine_factory")
def create_engine_factory():
    """
    Create class object
    """
    return EngineFactory()


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


def test_engine_factory_initialization(engine_factory):
    """
    Test engine_factory_initialization
    """
    assert engine_factory is not None
    assert engine_factory.connected is False
    assert engine_factory.loader_engine is None
    assert engine_factory.processing_role == "LOADER"
    assert engine_factory.schema_name == "saas_usage_ping"
