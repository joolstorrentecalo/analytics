# Write docstring for the EngineFactory class
"""
A factory class for managing connections to Snowflake and handling data uploads.

This class provides methods to:
1. Connect to a Snowflake database
2. Upload pandas DataFrames to Snowflake tables
3. Dispose of the database connection

Attributes:
    connected (bool): Indicates if a connection to Snowflake is currently established
    config_vars (dict): Configuration variables for the Snowflake connection
    loader_engine: The SQLAlchemy engine for database operations
    processing_role (str): The role used for database operations (default: "LOADER")
    schema_name (str): The schema name for data uploads (default: "saas_usage_ping")

Methods:
    connect(): Establishes a connection to Snowflake
    dispose(): Closes the current Snowflake connection
    upload_to_snowflake(table_name: str, data: pd.DataFrame): Uploads a DataFrame to a Snowflake table
"""
import pandas as pd
from gitlabdata.orchestration_utils import dataframe_uploader, snowflake_engine_factory
from os import environ as env

class EngineFactory:
    """
    Class to manage connection to Snowflake
    """

    def __init__(self):
        self.connected = False
        self.config_vars = env.copy()
        self.loader_engine = None
        self.processing_role = "LOADER"
        self.schema_name = "saas_usage_ping"

    def connect(self):
        """
        Connect to engine factory, return connection object
        """
        self.loader_engine = snowflake_engine_factory(
            self.config_vars, self.processing_role
        )
        self.connected = True

        return self.loader_engine.connect()

    def dispose(self) -> None:
        """
        Dispose from engine factory
        """
        if self.connected:
            self.loader_engine.dispose()

    def upload_to_snowflake(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Upload dataframe to Snowflake
        """
        dataframe_uploader(
            dataframe=data,
            engine=self.loader_engine,
            table_name=table_name,
            schema=self.schema_name,
        )