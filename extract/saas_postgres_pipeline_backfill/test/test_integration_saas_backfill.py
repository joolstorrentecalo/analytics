import os
import pytest
import re
import sys
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.engine.base import Engine

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = (
    abs_path[: abs_path.find("extract")]
    + "extract/saas_postgres_pipeline_backfill/postgres_pipeline/"
)
sys.path.append(abs_path)

from postgres_pipeline_table import PostgresPipelineTable
from utils import (
    # get_engines,
    postgres_engine_factory,
    manifest_reader,
    is_new_table,
    BACKFILL_METADATA_TABLE,
)


class TestBackfillIntegration:
    def setup(self):
        manifest_file_path = "extract/saas_postgres_pipeline_backfill/manifests_decomposed/el_gitlab_com_new_db_manifest.yaml"

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()
        self.metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]["postgres_metadata_connection"], env
        )
        metadata_schema = "saas_db_metadata"
        metadata_table = "backfill_metadata"
        self.test_metadata_table = f"test_{metadata_table}"
        self.test_metadata_table_full_path = (
            f"{metadata_schema}.{self.test_metadata_table}"
        )

        delete_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        create_table_query = f"""
        create table {self.test_metadata_table_full_path}
        (like {metadata_schema}.{metadata_table});
        """

        with self.metadata_engine.connect() as connection:
            connection.execute(delete_query)
            connection.execute(create_table_query)
            # connection.execute(alter_query)

    def teardown(self):
        manifest_file_path = "extract/saas_postgres_pipeline_backfill/manifests_decomposed/el_gitlab_com_new_db_manifest.yaml"

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()
        self.metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]["postgres_metadata_connection"], env
        )
        metadata_schema = "saas_db_metadata"
        metadata_table = "backfill_metadata"
        self.test_metadata_table = f"test_{metadata_table}"
        self.test_metadata_table_full_path = (
            f"{metadata_schema}.{self.test_metadata_table}"
        )

        delete_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        create_table_query = f"""
        create table {self.test_metadata_table_full_path}
        (like {metadata_schema}.{metadata_table});
        """

        with self.metadata_engine.connect() as connection:
            connection.execute(delete_query)
            connection.execute(create_table_query)
            # connection.execute(alter_query)

    def test_if_new_table_backfill(self):
        """
        When the metadata database is empty, ascertain that when
        backfilling 'some_table', it's considered a new table.

        After inserting the table into metadata, ascertain that
        it's no longer considered a new table
        """
        source_table = "some_table"

        # Test when table is missing in metadata
        result = is_new_table(
            self.metadata_engine, self.test_metadata_table, source_table
        )
        expected_result = True
        assert result == expected_result

        # Insert test record
        database_name = "some_db"
        initial_load_start_date = datetime(2023, 1, 1)
        upload_date = datetime(2023, 1, 1)
        upload_file_name = "some_file"
        last_extracted_id = 10
        max_id = 20
        is_export_completed = True
        chunk_row_count = 3

        insert_query = f"""
        INSERT INTO {self.test_metadata_table_full_path}
        VALUES (
            '{database_name}',
            '{source_table}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count});
        """

        # Test when table is inserted into metadata
        with self.metadata_engine.connect() as connection:
            connection.execute(insert_query)

        result = is_new_table(
            self.metadata_engine, self.test_metadata_table, source_table
        )
        expected_result = False
        assert result == expected_result

    @patch("postgres_pipeline_table.is_new_table")
    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_remove_unprocessed_new_table(
        self, mock_remove_unprocessed_files, mock_is_new_table
    ):
        """
        Test that when is_new_table() is True, that
        remove_unprocessed_files_from_gcs() is called
        """

        # Create a mock source_engine and metadata_engine objects
        source_engine = MagicMock(spec=Engine)
        metadata_engine = MagicMock(spec=Engine)

        # Create a mock PostgresPipelineTable object
        table_config = {
            "import_query": "SELECT * FROM some_table",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
        }
        pipeline_table = PostgresPipelineTable(table_config)

        mock_is_new_table.return_value = True
        # Call the function being tested
        pipeline_table.check_is_backfill_needed(source_engine, metadata_engine)

        # Assert that remove_unprocessed_files_from_gcs was called with the correct arguments
        mock_remove_unprocessed_files.assert_called_once_with(
            BACKFILL_METADATA_TABLE, pipeline_table.source_table_name
        )

    @patch("postgres_pipeline_table.schema_addition_check")
    @patch("postgres_pipeline_table.is_new_table")
    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_remove_unprocessed_schema_change(
        self,
        mock_remove_unprocessed_files,
        mock_is_new_table,
        mock_schema_addition_check,
    ):
        """
        Test that when there is a schema addition, that
        remove_unprocessed_files_from_gcs() is called
        """

        # Create a mock source_engine and metadata_engine objects
        source_engine = MagicMock(spec=Engine)
        metadata_engine = MagicMock(spec=Engine)

        # Create a mock PostgresPipelineTable object
        table_config = {
            "import_query": "SELECT * FROM some_table",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
        }
        pipeline_table = PostgresPipelineTable(table_config)

        mock_is_new_table.return_value = False
        mock_schema_addition_check.return_value = True

        # Call the function being tested
        pipeline_table.check_is_backfill_needed(source_engine, metadata_engine)

        # Assert that remove_unprocessed_files_from_gcs was called with the correct arguments
        mock_remove_unprocessed_files.assert_called_once_with(
            BACKFILL_METADATA_TABLE, pipeline_table.source_table_name
        )

    """
    def test_if_in_middle_of_backfill_more_than_24hr_since_last_write(self):
        # Arrange
        # Code to simulate being in the middle of a backfill with more than 24 hours since the last write.

        # Act
        # Code to restart the backfill.

        # Assert
        # Code to verify that the backfill was successful and started over from the beginning.

    def test_dont_backfill_if_conditions_not_met(self):
        # Arrange
        # Code to simulate a scenario where the backfill conditions are not met.

        # Act
        # Code to attempt to backfill the table.

        # Assert
        # Code to verify that the backfill was not attempted.

    def test_row_counts_match_for_ci_triggers_table(self):
        # Arrange
        # Code to prepare test data and environment.

        # Act
        # Code to backfill the table and count the rows in the resulting Parquet file.

        # Assert
        # Code to verify that the row counts in the Parquet file and source match.
        """
