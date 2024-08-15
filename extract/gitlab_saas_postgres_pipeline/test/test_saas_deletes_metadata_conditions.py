import os
import pendulum
from datetime import datetime, timedelta
from sqlalchemy.engine.base import Engine
from unittest.mock import MagicMock, patch

from postgres_pipeline_table import PostgresPipelineTable
from postgres_utils import (
    DELETE_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    METADATA_SCHEMA,
    postgres_engine_factory,
    manifest_reader,
)

# needed for `get_is_past_due_deletes()`
os.environ["DATE_INTERVAL_END"] = pendulum.parse("2024-01-01").to_iso8601_string()


def insert_into_metadata_db(metadata_engine, full_table_path, metadata):
    insert_query = f"""
    INSERT INTO {full_table_path}
    VALUES (
        '{metadata["database_name"]}',
        '{metadata["real_target_table"]}',
        '{metadata["initial_load_start_date"]}',
        '{metadata["upload_date"]}',
        '{metadata["upload_file_name"]}',
        {metadata["last_extracted_id"]},
        {metadata["max_id"]},
        {metadata["is_export_completed"]},
        {metadata["chunk_row_count"]});
    """
    print(f"\ninsert_query: {insert_query}")
    # Test when table is inserted into metadata
    with metadata_engine.connect() as connection:
        connection.execute(insert_query)


def setup_table(metadata_engine, metadata_schema, metadata_table):
    test_metadata_deletes_table = f"test_{metadata_table}"

    test_metadata_deletes_table_full_path = (
        f"{metadata_schema}.{test_metadata_deletes_table}"
    )
    drop_query = f""" drop table if exists {test_metadata_deletes_table_full_path}"""

    create_table_query = f"""
    create table {test_metadata_deletes_table_full_path}
    (like {metadata_schema}.{metadata_table});
    """

    with metadata_engine.connect() as connection:
        connection.execute(drop_query)
        print(f"\ncreate_table_query: {create_table_query}")
        connection.execute(create_table_query)
    return test_metadata_deletes_table, test_metadata_deletes_table_full_path


class TestCheckDeletes:
    def setup(self):
        """
        - Create test metdata table
        - Create a mock PostgresPipelineTable object
        """
        manifest_connection_info_file_path = "extract/gitlab_saas_postgres_pipeline/manifests/el_saas_connection_info.yaml"
        manifest_dict_connection_info = manifest_reader(
            manifest_connection_info_file_path
        )
        env = os.environ.copy()

        # Create a mock source/self.target_engine
        self.source_engine = MagicMock(spec=Engine)
        self.target_engine = MagicMock(spec=Engine)

        self.metadata_engine = postgres_engine_factory(
            manifest_dict_connection_info["connection_info"][
                "postgres_metadata_connection"
            ],
            env,
        )

        # set-up delete
        (
            self.test_metadata_deletes_table,
            self.test_metadata_deletes_table_full_path,
        ) = setup_table(self.metadata_engine, METADATA_SCHEMA, DELETE_METADATA_TABLE)

        table_config = {
            "import_query": "SELECT * FROM some_table;",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
        }
        self.pipeline_table = PostgresPipelineTable(table_config)

    def teardown(self):
        for table in [
            self.test_metadata_deletes_table_full_path,
        ]:
            drop_query = f""" drop table if exists {table}"""

            with self.metadata_engine.connect() as connection:
                connection.execute(drop_query)

    def test_check_deletes_hanging_deletes_continue(self):
        initial_load_start_date = datetime(2023, 1, 2)
        last_extracted_id = 10
        metadata = {
            "real_target_table": self.pipeline_table.get_target_table_name(),
            "database_name": "some_db",
            "initial_load_start_date": initial_load_start_date,
            "upload_date": datetime.utcnow() - timedelta(hours=18),
            "upload_file_name": "some_file",
            "last_extracted_id": last_extracted_id,
            "max_id": 20,
            "is_export_completed": False,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_deletes_table_full_path, metadata
        )

        (
            is_export_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_deletes(
            self.metadata_engine, self.test_metadata_deletes_table
        )
        assert is_export_needed is True
        assert initial_load_start_date == initial_load_start_date
        assert start_pk == last_extracted_id + 1

    def test_check_deletes_hanging_deletes_abort(self):
        initial_load_start_date = datetime(2023, 1, 2)
        last_extracted_id = 10
        metadata = {
            "real_target_table": self.pipeline_table.get_target_table_name(),
            "database_name": "some_db",
            "initial_load_start_date": initial_load_start_date,
            "upload_date": datetime.utcnow() - timedelta(hours=30),
            "upload_file_name": "some_file",
            "last_extracted_id": last_extracted_id,
            "max_id": 20,
            "is_export_completed": False,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_deletes_table_full_path, metadata
        )

        (
            is_export_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_deletes(
            self.metadata_engine, self.test_metadata_deletes_table
        )
        assert is_export_needed is True
        assert initial_load_start_date is None
        assert start_pk == 1

    @patch("postgres_pipeline_table.is_resume_export")
    def test_check_deletes_last_export(self, mock_is_resume_export):
        mock_is_resume_export.return_value = False, None, 1

        upload_date = datetime(2999, 1, 1)
        metadata = {
            "real_target_table": self.pipeline_table.get_target_table_name(),
            "database_name": "some_db",
            "initial_load_start_date": datetime(2023, 1, 2),
            "upload_date": upload_date,
            "upload_file_name": "some_file",
            "last_extracted_id": 10,
            "max_id": 20,
            "is_export_completed": False,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_deletes_table_full_path, metadata
        )

        (
            is_export_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_deletes(
            self.metadata_engine, self.test_metadata_deletes_table
        )

        assert is_export_needed is True
        assert initial_load_start_date is None
        assert start_pk == 1
