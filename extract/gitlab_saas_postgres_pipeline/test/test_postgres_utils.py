""" Test postgres_utils.py """

import os
import re
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.engine.base import Engine
import pandas as pd
from datetime import datetime

from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    get_prefix,
    get_initial_load_prefix,
    get_upload_file_name,
    seed_and_upload_snowflake,
    INCREMENTAL_LOAD_TYPE_BY_ID,
    chunk_and_upload_metadata,
    check_and_handle_schema_removal,
    get_min_or_max_id,
    update_import_query_for_delete_export,
    upload_to_snowflake_after_extraction,
    is_delete_export_needed,
    get_is_past_due_deletes,
    range_generator,
)

CSV_CHUNKSIZE = 500
database_types = ["main", "cells", "ci"]


class TestPostgresUtils:
    def setup(self):
        pass

    def test_get_prefix(self):
        """
        Check that the prefix matches expected prefix
        """
        staging_or_processed = "staging"
        load_by_id_export_type = "backfill"
        table = "alerts"

        table = table.upper()  # test when passed in table is UPPER
        initial_load_prefix = datetime(2023, 1, 1).strftime("%Y-%m-%d")

        for database_type in database_types:
            actual = get_prefix(
                staging_or_processed=staging_or_processed,
                load_by_id_export_type=load_by_id_export_type,
                table=table,
                initial_load_prefix=initial_load_prefix,
                database_type=database_type,
            )
            if database_type == "cells":
                expected = f"{staging_or_processed}/{load_by_id_export_type}/cells/{table}/{initial_load_prefix}".lower()
            else:
                expected = f"{staging_or_processed}/{load_by_id_export_type}/{table}/{initial_load_prefix}".lower()
            assert actual == expected

    def test_get_initial_load_prefix(self):
        """
        Check that actual value matches expected value
        and that the prefix is all-lowercase
        """
        initial_load_start_date = datetime.utcnow()
        expected_initial_load_prefix = f"initial_load_start_{initial_load_start_date.isoformat(timespec='milliseconds')}".lower()
        actual_initial_load_prefix = get_initial_load_prefix(initial_load_start_date)
        assert expected_initial_load_prefix == actual_initial_load_prefix
        assert all([not char.isupper() for char in actual_initial_load_prefix])

    def test_get_upload_file_name(self):
        load_by_id_export_type = "backfill"
        table = "alerts"
        table = table.upper()  # test when passed in table is UPPER
        initial_load_start_date = datetime.utcnow()
        upload_date = datetime.utcnow()

        for database_type in database_types:
            actual = get_upload_file_name(
                load_by_id_export_type,
                table,
                initial_load_start_date,
                upload_date,
                database_type,
            )

            initial_load_start_date_iso = initial_load_start_date.isoformat(
                timespec="milliseconds"
            )
            upload_date_iso = upload_date.isoformat(timespec="milliseconds")
            if database_type == "cells":
                expected = f"staging/{load_by_id_export_type}/cells/{table}/initial_load_start_{initial_load_start_date_iso}/{upload_date_iso}_{table}.parquet.gzip".lower()
            else:
                expected = f"staging/{load_by_id_export_type}/{table}/initial_load_start_{initial_load_start_date_iso}/{upload_date_iso}_{table}.parquet.gzip".lower()
            upload_date_iso = upload_date.isoformat(timespec="milliseconds")
            assert actual == expected

    def test_seed_and_upload_snowflake(self):
        """Test that non-temp tables are aborted"""
        database_kwargs = {"target_table": "alerts"}
        for database_type in database_types:
            with pytest.raises(ValueError):
                seed_and_upload_snowflake(
                    None, None, database_kwargs, None, None, None, database_type
                )

    @patch("postgres_utils.write_metadata")
    @patch("postgres_utils.upload_to_snowflake_after_extraction")
    @patch("postgres_utils.upload_to_gcs")
    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata1(
        self,
        mock_read_sql_tmpfile,
        mock_upload_to_gcs,
        mock_upload_to_snowflake_after_extraction,
        mock_write_metadata,
    ):
        """
        Test that when we have not reached the max_source_id,
        that the files aren't uploaded to Snowflake
        """
        iter_csv = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
            chunksize=5,
        )
        mock_read_sql_tmpfile.return_value = iter_csv
        some_engine = MagicMock(spec=Engine)

        query = "select 1;"
        primary_key = "ID"
        max_source_id = 25
        initial_load_start_date = datetime.utcnow()
        database_kwargs = {
            "source_database": "some_db",
            "real_target_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = INCREMENTAL_LOAD_TYPE_BY_ID

        for database_type in database_types:
            returned_initial_load_start_date = chunk_and_upload_metadata(
                query,
                primary_key,
                database_type,
                max_source_id,
                initial_load_start_date,
                database_kwargs,
                CSV_CHUNKSIZE,
                load_by_id_export_type,
            )

            mock_upload_to_snowflake_after_extraction.assert_not_called()
            assert returned_initial_load_start_date == initial_load_start_date

    @patch("postgres_utils.write_metadata")
    @patch("postgres_utils.upload_to_snowflake_after_extraction")
    @patch("postgres_utils.upload_to_gcs")
    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata2(
        self,
        mock_read_sql_tmpfile,
        mock_upload_to_gcs,
        mock_upload_to_snowflake_after_extraction,
        mock_write_metadata,
    ):
        """
        Test INCREMENTAL_LOAD_TYPE_BY_ID

        Test that when we have reached the max_source_id,
        that the files ARE uploaded to Snowflake

        Should reach max_source_id after 4 loops and then terminate.
        Therefore, test that upload_to_gcs() and write_metadata()
        are called twice.

        Test that upload_to_snowflake_after_extraction() is called only once

        """
        iter_csv = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
            chunksize=5,
        )
        df = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv"
        )
        mock_read_sql_tmpfile.return_value = iter_csv
        some_engine = MagicMock(spec=Engine)

        query = "select 1;"
        primary_key = "ID"
        max_source_id = df[primary_key].max()
        initial_load_start_date = datetime.utcnow()
        database_kwargs = {
            "source_database": "some_db",
            "real_target_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = INCREMENTAL_LOAD_TYPE_BY_ID

        for database_type in database_types:
            returned_initial_load_start_date = chunk_and_upload_metadata(
                query,
                primary_key,
                database_type,
                max_source_id,
                initial_load_start_date,
                database_kwargs,
                CSV_CHUNKSIZE,
                load_by_id_export_type,
            )

            assert mock_upload_to_gcs.call_count == 4
            mock_write_metadata.assert_called_once()
            mock_upload_to_snowflake_after_extraction.assert_called_once()

            assert returned_initial_load_start_date == initial_load_start_date

    '''
    @patch("postgres_utils.snowflake_engine_factory")
    @patch("postgres_utils.seed_and_upload_snowflake")
    def test_upload_to_snowflake_after_extraction(
        self, mock_seed_and_upload_snowflake, mock_snowflake_engine_factory
    ):
        """
        Test LOAD_TYPE_BY_ID=BACKFILL

        Similiar to above test2, but make sure we call `seed_and_upload_snowflake`
        """
        chunk_df = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
        )

        some_engine = MagicMock(spec=Engine)
        mock_snowflake_engine_factory.return_value = some_engine

        initial_load_start_date = datetime.utcnow()
        database_kwargs = {
            "source_database": "some_db",
            "real_target_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = "backfill"
        advanced_metadata = None

        upload_to_snowflake_after_extraction(
            chunk_df,
            database_kwargs,
            load_by_id_export_type,
            initial_load_start_date,
            advanced_metadata,
        )
        mock_seed_and_upload_snowflake.assert_called_once()
    '''

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_no_change(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello", "world"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_not_called()

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_schema_addition(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello", "world", "manifest_addition"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_not_called()

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_schema_removal(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_called_once()

    @patch("postgres_utils.query_results")
    def test_get_min_or_max_id(self, mock_query_results):
        """
        This is a tough function to test because the main logic involves running the min()/max()
        against the sql engine

        Test the following:
            - query_results() is called with the correct query (including additional filter)
            - returns the first id in the dataframe
        """
        primary_key = "ID"
        engine = MagicMock(spec=Engine)
        table = "alerts"
        min_or_max = "min"
        additional_filtering = "AND id = 50"

        dataframe = pd.DataFrame({"ID": [1]})
        mock_query_results.return_value = dataframe

        min_id = get_min_or_max_id(
            primary_key, engine, table, min_or_max, additional_filtering
        )
        expected_id_query = (
            "SELECT COALESCE(min(ID), 0) as ID FROM alerts WHERE true AND id = 50;"
        )

        mock_query_results.assert_called_once_with(expected_id_query, engine)
        assert min_id == 1

    def test_update_import_query_for_delete_export(self):
        """
        For deletes, test that the import query is updated to reflect
        only the pk within the select
        """

        def clean_res(res):
            res = res.replace("\n", " ")
            res = re.sub(" +", " ", res)
            return res

        # Query 1
        import_query = """SELECT id
        , created_at
        , updated_at
        , relative_position
        , start_event_identifier
        , end_event_identifier
        , group_id
        , start_event_label_id
        , end_event_label_id
        , hidden
        , custom
        , name
        , group_value_stream_id
        FROM analytics_cycle_analytics_group_stages
        WHERE updated_at BETWEEN '{BEGIN_TIMESTAMP}'::timestamp
          AND '{END_TIMESTAMP}'::timestamp"""
        primary_key = "id"

        # Call the function with the test data
        updated_query = update_import_query_for_delete_export(import_query, primary_key)

        # Assert that the function returns the expected result
        expected_query = "SELECT id FROM  analytics_cycle_analytics_group_stages\nWHERE updated_at BETWEEN '{BEGIN_TIMESTAMP}'::timestamp\n  AND '{END_TIMESTAMP}'::timestamp"

        expected_query_cleaned = clean_res(expected_query)
        updated_query_cleaned = clean_res(updated_query)
        assert expected_query_cleaned == updated_query_cleaned

        # Query 2
        import_query = """SELECT id
        , CAST(sha AS VARCHAR) AS sha
        , issue_id
        , created_at
        , author_id
        FROM design_management_versions"""

        primary_key = "id"

        # Call the function with the test data
        updated_query = update_import_query_for_delete_export(import_query, primary_key)

        # Assert that the function returns the expected result
        expected_query = "SELECT id FROM design_management_versions"

        expected_query_cleaned = clean_res(expected_query)
        updated_query_cleaned = clean_res(updated_query)
        assert expected_query_cleaned == updated_query_cleaned

    @patch("postgres_utils.get_is_past_due_deletes")
    @patch("postgres_utils.query_backfill_status")
    def test_is_delete_export_needed(
        self, mock_query_backfill_status, mock_get_is_past_due_deletes
    ):
        """Check that is_export_needed is correct"""
        is_export_completed = True
        prev_initial_load_start_date = None
        last_extracted_id = 1
        last_upload_date = datetime.utcnow()

        mock_query_backfill_status.return_value = [
            (
                is_export_completed,
                prev_initial_load_start_date,
                last_extracted_id,
                last_upload_date,
            )
        ]
        engine = MagicMock(spec=Engine)

        # Test 1: check that if past due, then deletes export needed
        mock_get_is_past_due_deletes.return_value = True
        res = is_delete_export_needed(
            engine, "some_metadata_table", "some_target_table"
        )
        assert res is True

        # Test 2: check that if not past due, then NO deletes export needed
        mock_get_is_past_due_deletes.return_value = False
        res = is_delete_export_needed(
            engine, "some_metadata_table", "some_target_table"
        )
        assert res is False

    def test_get_is_past_due_deletes(self):
        """
        This one is hard to test because the cronitor date is dynamic
        At least make sure that if the last load is in the future,
        that the delete is not necessary.
        """
        prev_initial_load_start_date = datetime(2999, 12, 31)
        is_past_due = get_is_past_due_deletes(prev_initial_load_start_date)
        assert is_past_due is False

    def test_range_generator(self):
        # 1) stop is less than step
        start, stop, step = 1, 1, 300
        id_pairs = []
        for id_pair in range_generator(start, stop, step):
            id_pairs.append(id_pair)

        assert id_pairs == [(start, step)]

        # 2) stop is on one of the steps
        start, stop, step = 1, 600, 300
        id_pairs = []
        for id_pair in range_generator(start, stop, step):
            id_pairs.append(id_pair)

        assert id_pairs == [(start, step), (step + 1, step * 2)]

        # check that the pairs don't overlap
        first_pair_stop = id_pairs[0][1]
        second_pair_start = id_pairs[1][0]
        assert first_pair_stop + 1 == second_pair_start

        # 3) stop is not on one of the steps
        start, stop, step = 1, 602, 300
        id_pairs = []
        for id_pair in range_generator(start, stop, step):
            id_pairs.append(id_pair)

        assert id_pairs == [
            (start, step),
            (step + 1, step * 2),
            (step * 2 + 1, step * 3),
        ]

        # check that the pairs don't overlap
        first_pair_stop = id_pairs[0][1]
        second_pair_start = id_pairs[1][0]
        assert first_pair_stop + 1 == second_pair_start
