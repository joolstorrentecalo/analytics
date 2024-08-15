"""
Test the functions in postgres_pipeline_table.py
"""

from datetime import datetime
from unittest.mock import MagicMock, patch
from sqlalchemy.engine.base import Engine

from postgres_pipeline_table import PostgresPipelineTable

database_types = ["main", "cells", "ci"]


class TestPostgresPipelineTable:
    def setup(self):
        table_config = {
            "import_query": "SELECT * FROM some_table;",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
            "database_type": "main",
        }
        self.pipeline_table = PostgresPipelineTable(table_config)
        # Create a mock source/self.target_engine
        self.engine = MagicMock(spec=Engine)

    def test_is_scd(self):
        """
        Test that
        """
        is_scd = self.pipeline_table.is_scd()
        is_incremental = self.pipeline_table.is_incremental()
        assert is_scd
        assert is_incremental is False

        table_config2 = {
            "import_query": "SELECT * FROM some_table WHERE updated_at >= '{BEGIN_TIMESTAMP}'::timestamp;",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
            "database_type": "main",
        }
        pipeline_table2 = PostgresPipelineTable(table_config2)
        is_scd = pipeline_table2.is_scd()
        is_incremental = pipeline_table2.is_incremental()
        assert is_scd is False
        assert is_incremental

    def test_get_source_table_name(self):
        """check that get_target_name() matches its attribute"""

        # test when 'import_query_table' == manifest['export_table']
        actual_source_table_name = self.pipeline_table.source_table_name
        expected_source_table_name = self.pipeline_table.table_dict["export_table"]
        assert actual_source_table_name == expected_source_table_name

        # test when 'import_query_table' != manifest['export_table']
        table_config2 = {
            "import_query": "SELECT * FROM p_ci_builds WHERE updated_at >= '{BEGIN_TIMESTAMP}'::timestamp;",
            "import_db": "some_database",
            "export_table": "ci_builds",
            "export_table_primary_key": "id",
            "database_type": "ci",
        }
        pipeline_table2 = PostgresPipelineTable(table_config2)
        actual_source_table_name = pipeline_table2.source_table_name
        expected_source_table_name = "p_" + pipeline_table2.table_dict["export_table"]
        assert actual_source_table_name == expected_source_table_name

    def test_get_target_table_name(self):
        """check that get_target_name() matches its attribute"""
        actual_table_name = self.pipeline_table.get_target_table_name()
        expected_table_name = self.pipeline_table.target_table_name
        assert actual_table_name == expected_table_name

    def test_get_temp_target_table_name(self):
        """
        Test that get_temp_target_table_name() adds a TEMP to the table_name
        """
        actual = self.pipeline_table.get_temp_target_table_name()
        expected = f"{self.pipeline_table.get_target_table_name()}_TEMP"
        assert actual == expected

    def test_internal_table_names(self):
        """Test that internal tables are handled correctly"""
        table_config = {
            "import_query": "SELECT * FROM some_table;",
            "import_db": "some_database",
            "export_table": "some_table_internal_only",
            "export_table_primary_key": "id",
        }
        self.pipeline_table = PostgresPipelineTable(table_config)
        assert "_internal_only" not in self.pipeline_table.source_table_name
        assert "_internal_only" in self.pipeline_table.target_table_name.lower()

    @patch("postgres_pipeline_table.swap_temp_table")
    def test_swap_temp_table_on_schema_change_continue(self, mock_swap_temp_table):
        """If is_schema_addition=True & loaded=True, swap_temp_table"""
        is_schema_addition = True
        loaded = True
        self.pipeline_table.swap_temp_table_on_schema_change(
            is_schema_addition, loaded, self.engine
        )

        mock_swap_temp_table.assert_called_once_with(
            self.engine,
            self.pipeline_table.get_target_table_name(),
            self.pipeline_table.get_temp_target_table_name(),
        )

    @patch("postgres_pipeline_table.swap_temp_table")
    def test_swap_temp_table_on_schema_change_stop1(self, mock_swap_temp_table):
        """If is_schema_addition=False, don't run swap_temp_table"""
        is_schema_addition = False
        loaded = True
        self.pipeline_table.swap_temp_table_on_schema_change(
            is_schema_addition, loaded, self.engine
        )

        mock_swap_temp_table.assert_not_called()

    @patch("postgres_pipeline_table.swap_temp_table")
    def test_swap_temp_table_on_schema_change_stop2(self, mock_swap_temp_table):
        """If loaded=False, don't run swap_temp_table"""
        is_schema_addition = True
        loaded = False
        self.pipeline_table.swap_temp_table_on_schema_change(
            is_schema_addition, loaded, self.engine
        )

        mock_swap_temp_table.assert_not_called()

    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.swap_temp_table_on_schema_change"
    )
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_is_new_table_or_schema_addition"
    )
    @patch("postgres_pipeline_table.PostgresPipelineTable._do_load_by_id")
    @patch("postgres_pipeline_table.PostgresPipelineTable.check_backfill_metadata")
    def test_do_load_backfill(
        self,
        mock_check_backfill_metadata,
        mock__do_load_by_id,
        mock_check_is_new_table_or_schema_addition,
        mock_swap_temp_table_on_schema_change,
    ):
        """
        Test that on backfill,
        check_is_new_table_or_schema_addition() is not called
        and swap_temp_table_on_schema_change() is not called
        """
        load_type = "backfill"
        database_type = ["main", "cells", "ci"]
        source_engine = target_engine = metadata_engine = self.engine

        mock_check_backfill_metadata.return_value = True, datetime.utcnow(), -1
        for db in database_type:
            self.pipeline_table.do_load(
                load_type, source_engine, target_engine, metadata_engine, db
            )
        mock_check_is_new_table_or_schema_addition.assert_not_called()
        mock_swap_temp_table_on_schema_change.assert_not_called()

    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.swap_temp_table_on_schema_change"
    )
    @patch("postgres_pipeline_table.load_functions.load_incremental")
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_and_handle_schema_removal"
    )
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_is_new_table_or_schema_addition"
    )
    def test_do_load_incremental_regular(
        self,
        mock_check_is_new_table_or_schema_addition,
        mock_check_and_handle_schema_removal,
        mock_load_incremental,
        mock_swap_temp_table_on_schema_change,
    ):
        """Test that on incremental load,
        1. check_is_new_table_or_schema_addition() is called
        2. check_and_handle_schema_removal() is called once
        3. load_incremental() is called once
        4. swap_temp_table_on_schema_change() is called once

        and swap_temp_table_on_schema_change() is called
        """
        load_type = "incremental"
        source_engine = target_engine = metadata_engine = self.engine
        is_schema_addition = False
        loaded = True

        mock_check_is_new_table_or_schema_addition.return_value = is_schema_addition
        self.pipeline_table.incremental_type = "load_by_date"
        mock_load_incremental.return_value = loaded

        for database_type in database_types:
            self.pipeline_table.do_load(
                load_type, source_engine, target_engine, metadata_engine, database_type
            )
            mock_load_incremental.assert_called_with(
                source_engine,
                target_engine,
                self.pipeline_table.source_table_name,
                self.pipeline_table.table_dict,
                self.pipeline_table.get_target_table_name(),
                database_type,
            )
        mock_check_is_new_table_or_schema_addition.assert_called_with(
            source_engine, target_engine
        )
        mock_check_and_handle_schema_removal.assert_called_with(
            source_engine, target_engine
        )

        mock_swap_temp_table_on_schema_change.assert_called_with(
            is_schema_addition, loaded, target_engine
        )

    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.swap_temp_table_on_schema_change"
    )
    @patch("postgres_pipeline_table.load_functions.load_incremental")
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_and_handle_schema_removal"
    )
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_is_new_table_or_schema_addition"
    )
    def test_do_load_incremental_schema_change(
        self,
        mock_check_is_new_table_or_schema_addition,
        mock_check_and_handle_schema_removal,
        mock_load_incremental,
        mock_swap_temp_table_on_schema_change,
    ):
        """Test that on incremental load, where schema has changed that:
        1. check_is_new_table_or_schema_addition() is called
        2. loaded=False
        3. check_and_handle_schema_removal() is not called
        4. swap_temp_table_on_schema_change() is called
        """
        load_type = "incremental"
        source_engine = target_engine = metadata_engine = self.engine
        is_schema_addition = True

        mock_check_is_new_table_or_schema_addition.return_value = is_schema_addition
        self.pipeline_table.incremental_type = "load_by_date"

        for database_type in database_types:
            loaded = self.pipeline_table.do_load(
                load_type, source_engine, target_engine, metadata_engine, database_type
            )
            assert not loaded
        mock_check_is_new_table_or_schema_addition.assert_called_with(
            source_engine, target_engine
        )
        mock_check_and_handle_schema_removal.assert_not_called()
        mock_load_incremental.assert_not_called()
        mock_swap_temp_table_on_schema_change.assert_called_with(
            is_schema_addition, loaded, target_engine
        )

    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.swap_temp_table_on_schema_change"
    )
    @patch("postgres_pipeline_table.PostgresPipelineTable.do_scd")
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_and_handle_schema_removal"
    )
    @patch(
        "postgres_pipeline_table.PostgresPipelineTable.check_is_new_table_or_schema_addition"
    )
    def test_do_load_scd(
        self,
        mock_check_is_new_table_or_schema_addition,
        mock_check_and_handle_schema_removal,
        mock_do_scd,
        mock_swap_temp_table_on_schema_change,
    ):
        """Test that on scd load
        do_scd() is called
        check_is_new_table_or_schema_addition() is called
        swap_temp_table_on_schema_change() is called
        """
        load_type = "scd"
        source_engine = target_engine = metadata_engine = self.engine
        is_schema_addition = False
        loaded = True

        mock_check_is_new_table_or_schema_addition.return_value = is_schema_addition
        self.pipeline_table.incremental_type = "load_by_date"
        mock_do_scd.return_value = loaded

        for database_type in database_types:
            self.pipeline_table.do_load(
                load_type, source_engine, target_engine, metadata_engine, database_type
            )
            mock_do_scd.assert_called_with(
                source_engine, target_engine, is_schema_addition, database_type
            )
        mock_check_is_new_table_or_schema_addition.assert_called_with(
            source_engine, target_engine
        )
        mock_check_and_handle_schema_removal.assert_called_with(
            source_engine, target_engine
        )
        mock_swap_temp_table_on_schema_change.assert_called_with(
            is_schema_addition, loaded, target_engine
        )
