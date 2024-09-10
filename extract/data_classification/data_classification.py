"""
Util unit for data classification
"""

import json
import os
import sys
from json import JSONDecodeError
from logging import error, info
from typing import List

import pandas as pd
import yaml
from gitlabdata.orchestration_utils import dataframe_uploader, snowflake_engine_factory
from sqlalchemy.engine.base import Engine


class DataClassification:
    """
    Main class for data classification for:
    - PII
    - MNPI
    """

    def __init__(
        self, tagging_type: str, mnpi_raw_file: str, incremental_load_days: int
    ):
        """
        Define parameters
        """

        self.encoding = "utf8"
        self.schema_name = "data_classification"
        self.table_name = "sensitive_objects_classification"
        self.processing_role = "SYSADMIN"
        self.loader_engine: Engine = None
        self.connected = False
        self.specification_file = "../../extract/data_classification/specification.yml"
        self.tagging_type = tagging_type
        self.mnpi_raw_file = mnpi_raw_file
        self.config_vars = os.environ.copy()
        self.incremental_load_days = incremental_load_days
        self.raw = self.config_vars["SNOWFLAKE_LOAD_DATABASE"]
        self.prep = self.config_vars["SNOWFLAKE_PREP_DATABASE"]
        self.prod = self.config_vars["SNOWFLAKE_PROD_DATABASE"]

    def __connect(self) -> Engine:
        """
        Connect to engine factory, return connection object
        """
        if not self.connected:
            self.loader_engine = snowflake_engine_factory(
                self.config_vars, self.processing_role
            )
            self.connected = True
            return self.loader_engine.connect()

    def __dispose(self) -> None:
        """
        Dispose from engine factory
        """
        if self.connected:
            self.connected = False
            self.loader_engine.dispose()

    def upload_to_snowflake(self) -> None:
        """
        Upload dataframe to Snowflake
        """
        try:
            dataframe_uploader(
                dataframe=self.identify_mnpi_data,
                engine=self.loader_engine,
                table_name=self.table_name,
                schema=self.schema_name,
            )
        except Exception as e:
            error(f"Error uploading to Snowflake: {e.__class__.__name__} - {e}")
            sys.exit(1)

    @staticmethod
    def quoted(input_str: str) -> str:
        """
        Returns input string with single quote
        """
        return "'" + input_str + "'"

    @staticmethod
    def double_quoted(input_str: str) -> str:
        """
        Returns input string with double quote
        """
        return '"' + input_str + '"'

    def load_mnpi_list(self) -> list:
        """
        Load MNPI list generated via dbt command
        """

        if not os.path.exists(self.mnpi_raw_file):
            error(f"File {self.mnpi_raw_file} is not generated, stopping processing")
            sys.exit(1)

        with open(self.mnpi_raw_file, mode="r", encoding=self.encoding) as file:
            res = []
            for line in file:
                try:
                    res.append(json.loads(line.rstrip()))
                except JSONDecodeError as e:
                    info(f"Wrong json format, discard and continue (expected): {e}")
                except Exception as e:
                    error(f"Exception: {e.__class__.__name__} - {e}")
                    sys.exit(1)
            return res

    def transform_mnpi_list(self, mnpi_list: list) -> list:
        """
        Transform MNPI list to uppercase in a proper format
        """

        def extract_full_path(configuration: dict) -> List[str]:
            database_name = configuration["config"]["database"].upper()
            schema_name = configuration["config"]["schema"].upper()
            alias = configuration["alias"].upper()

            return [
                database_name,
                schema_name,
                alias,
            ]

        return [extract_full_path(x) for x in mnpi_list]

    def get_pii_scope(self, scope_type: str) -> str:
        """
        This method get_pii_scope is responsible for generating a SQL WHERE clause for filtering
        PII (Personally Identifiable Information) data based on specified scopes.
        The method takes one parameter:
            scope_type: Determines whether to include or exclude data ("include" or "exclude")

        Get the WHERE clause for the PII data, as the scope type can be included/excluded, both logic is supported.

        The specification file (specification.yml) is exposed in the format
          PII:
            description: "PII data classification"
            include:
              databases:
                - PREP
                - RAW
                - PROD
              schemas:
                - PREP.*
                - RAW.*
                - PROD.*

              tables:
                - PREP.*.*
                - RAW.*.*
                - PROD.*.*
            exclude:
              databases:
                - NONE
              schemas:
                - NONE.*
              tables:
                - NONE.*.*
        and based on this logic, the WHERE clause of the SQL statement is generated bot for include and exclude clauses
        include - this section is mandatory to be specified in the specification.yml
        exclude - this section is optional

        The method then builds the WHERE clause in three parts:

        a. For databases:
            If databases are specified, it creates a condition to include/exclude those databases.
        b. For schemas:
            If schemas are specified, it adds conditions for each schema.
            It handles wildcards (*) in schema names, using ILIKE for pattern matching.
        c. For tables:
            If tables are specified, it adds conditions for each table.
            It handles wildcards (*) in table names, supporting both single and double wildcard patterns.

        Finally, it returns the complete WHERE clause as a string, prefixed with "AND" (or "NOT" if the scope is "excluded") and enclosed in parentheses.
        """
        res = ""
        scope = self.scope.get("data_classification").get("PII").get(scope_type)
        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")
        exclude_statement = "NOT" if scope_type == "exclude" else ""

        # Create part of the WHERE clause for databases
        # Result is in format: table_catalog [NOT] IN ('RAW','PREP')
        if databases:
            res = f" (table_catalog {exclude_statement} IN ({', '.join(self.quoted(x) for x in databases)}))"

        # Create part of the WHERE clause for schemas
        # Result is in format:
        #     RAW.*        -> table_catalog = 'RAW' AND table_schema ILIKE '%'
        #     RAW.SCHEMA_A -> table_catalog = 'RAW' AND table_schema = 'SCHEMA_A'
        if schemas:
            res += " AND"

            if exclude_statement:
                res += f" {exclude_statement}"

            for i, schema in enumerate(schemas, start=1):
                schema_list = schema.split(".")

                res += (
                    f" (table_catalog = {self.quoted(schema_list[0])} AND table_schema"
                )
                if "*" in schema_list:
                    res += f" ILIKE {self.quoted('%')})"
                else:
                    res += f" = {self.quoted(schema_list[1])})"

                if i < len(schemas):
                    res += " OR"

        # Create part of the WHERE clause for tables
        # Result is in format:
        #     RAW.*.*              -> table_catalog = 'RAW' AND table_schema ILIKE '%' AND table_name ILIKE '%'
        #     RAW.SCHEMA_A.*       -> table_catalog = 'RAW' AND table_schema = 'SCHEMA_A' AND table_name ILIKE '%'
        #     RAW.SCHEMA_A.TABLE_A -> table_catalog = 'RAW' AND table_schema = 'SCHEMA_A' AND table_name = 'TABLE_A'
        if tables:
            res += " AND"
            if exclude_statement:
                res += f" {exclude_statement}"
            for i, table in enumerate(tables, start=1):
                table_list = table.split(".")

                if "*" in table_list:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND table_schema"
                    if table_list.count("*") == 1:
                        res += f" = {self.quoted(table_list[1])} AND table_name ILIKE {self.quoted('%')})"
                    if table_list.count("*") == 2:
                        res += f" ILIKE {self.quoted('%')} AND table_name ILIKE {self.quoted('%')})"
                else:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND table_schema = {self.quoted(table_list[1])} and table_name = {self.quoted(table_list[2])})"

                if i < len(tables):
                    res += " OR"

        return f"AND ({res})"

    def _get_pii_select_part_query(self, database_name: str):
        """
        Return SELECT part of the PII INSERT query to avoid repetition
        """
        section = "PII"
        return (
            f"SELECT {self.quoted(section)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, REPLACE(table_type,'BASE TABLE','TABLE') AS table_type, DATE_PART(epoch_second, CURRENT_TIMESTAMP()) "
            f"  FROM {self.double_quoted(database_name)}.information_schema.tables "
            f" WHERE table_schema != 'INFORMATION_SCHEMA' "
        )

    @property
    def pii_query(self) -> str:
        """
        Property method for the query generated to define the scope
        for tagging PII data
        """

        insert_statement = (
            f"INSERT INTO {self.schema_name}.{self.table_name}(classification_type, created, last_altered,last_ddl, database_name, schema_name, table_name, table_type, _uploaded_at) "
            f"WITH base AS ("
            f"{self._get_pii_select_part_query(database_name=self.raw)} "
            f" UNION "
            f"{self._get_pii_select_part_query(database_name=self.prep)} "
            f" UNION "
            f"{self._get_pii_select_part_query(database_name=self.prod)} "
            f") "
            f"SELECT *"
            f"  FROM base"
            f" WHERE 1=1 "
        )

        where_clause_include = self.get_pii_scope(scope_type="include")
        where_clause_exclude = self.get_pii_scope(scope_type="exclude")

        res = f"{insert_statement}{where_clause_include}{where_clause_exclude}"
        return res

    def _get_mnpi_select_part_query(self, database_name: str):
        """
        Return SELECT part of the MNPI MERGE query to avoid repetition
        """
        return (
            "SELECT 'MNPI' AS classification_type, "
            "       created, "
            "       last_altered, "
            "       last_ddl, "
            "       table_catalog, "
            "       table_schema, "
            "       table_name, "
            "       REPLACE(table_type,'BASE TABLE','TABLE') AS table_type "
            f"  FROM {self.double_quoted(database_name)}.information_schema.tables "
            " WHERE table_schema != 'INFORMATION_SCHEMA' "
            "   AND table_catalog IN (SELECT database_name FROM database_list) "
        )

    @property
    def mnpi_metadata_update_query(self) -> str:
        """
        Generate update statement for the MNPI metadata
        """
        return (
            f"MERGE INTO {self.schema_name}.{self.table_name} USING ( "
            f"WITH database_list AS (SELECT DISTINCT database_name FROM  {self.schema_name}.{self.table_name}) "
            f"{self._get_mnpi_select_part_query(database_name=self.raw)} "
            " UNION "
            f"{self._get_mnpi_select_part_query(database_name=self.prep)} "
            " UNION "
            f"{self._get_mnpi_select_part_query(database_name=self.prod)} )"
            f" AS full_table_list "
            f" ON full_table_list.classification_type                  = {self.table_name}.classification_type "
            f"AND full_table_list.table_catalog                        = {self.table_name}.database_name "
            f"AND full_table_list.table_schema                         = {self.table_name}.schema_name "
            f"AND full_table_list.table_name                           = {self.table_name}.table_name "
            f"AND {self.table_name}.classification_type = 'MNPI' "
            "WHEN MATCHED THEN "
            "UPDATE "
            f"   SET {self.table_name}.created      = full_table_list.created, "
            f"       {self.table_name}.last_altered = full_table_list.last_altered, "
            f"       {self.table_name}.last_ddl     = full_table_list.last_ddl, "
            f"       {self.table_name}.table_type   = full_table_list.table_type"
        )

    @property
    def delete_data_query(self) -> str:
        """
        Query to delete data from table
        """
        return f"DELETE FROM {self.schema_name}.{self.table_name}"

    def classify_query(
        self, date_from: str, unset: str = "FALSE", tagging_type: str = "INCREMENTAL"
    ) -> str:
        """
        Query to call procedure with parameters for classification
        """
        return (
            f"CALL {self.double_quoted(self.raw)}.{self.schema_name}.execute_data_classification("
            f"p_type => {self.quoted(tagging_type)}, "
            f"p_date_from=>{self.quoted(date_from)} , "
            f"p_unset=> {self.quoted(unset)})"
        )

    def get_mnpi_scope(self, scope_type: str, row: list) -> bool:
        """
        Define what is included and what is excluded for MNPI data
        DATABASE, SCHEMA and TABLE level
        """
        section = "MNPI"
        scope = self.scope.get("data_classification").get(section).get(scope_type)

        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")

        database_name = str(row[0]).upper()
        schema_name = str(row[1]).upper()
        table_name = str(row[2]).upper()

        if database_name in databases:
            if (
                f"{database_name}.{schema_name}" in schemas
                or f"{database_name}.*" in schemas
            ):
                full_name = f"{database_name}.{schema_name}.{table_name}"

                if (
                    full_name in tables
                    or f"{database_name}.{schema_name}.*" in tables
                    or f"{database_name}.*.*" in tables
                ):

                    return True

        return False

    def filter_mnpi_data(self, mnpi_data: list) -> list:
        """
        filtering data based on the configuration
        """
        section = "MNPI"
        res = []

        for row in mnpi_data:

            include = self.get_mnpi_scope(scope_type="include", row=row)
            exclude = self.get_mnpi_scope(scope_type="exclude", row=row)

            if include and not exclude:
                null_value = None
                row = [
                    section,
                    null_value,
                    null_value,
                    null_value,
                    row[0],
                    row[1],
                    row[2],
                    null_value,
                ]
                res.append(row)
        return res

    @property
    def identify_mnpi_data(self) -> pd.DataFrame:
        """
        Entry point to identify MNPI data
        """
        mnpi_list = self.load_mnpi_list()
        mnpi_data = self.transform_mnpi_list(mnpi_list=mnpi_list)
        mnpi_data_filtered = self.filter_mnpi_data(mnpi_data=mnpi_data)

        columns = [
            "classification_type",
            "created",
            "last_altered",
            "last_ddl",
            "database_name",
            "schema_name",
            "table_name",
            "table_type",
        ]

        return pd.DataFrame(data=mnpi_data_filtered, columns=columns)

    @property
    def scope(self):
        """
        Open and load specification file with the definition what to include and what to exclude
        on DATABASE, SCHEMA and TABLE level
        """
        with open(
            file=self.specification_file, mode="r", encoding=self.encoding
        ) as file:
            manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

        return manifest_dict

    def classify(
        self, date_from: str, unset: str = "FALSE", tagging_type: str = "INCREMENTAL"
    ):
        """
        Routine to classify all data
        using stored procedure
        """
        info("START classify.")
        query = self.classify_query(
            date_from=date_from, unset=unset, tagging_type=tagging_type
        )
        info(f"....Call stored procedure: {query}")
        self.__execute_query(query=query)
        info("END classify.")

    def __execute_query(self, query: str):
        """
        Execute SQL query
        """
        try:
            connection = self.__connect()
            res = connection.execute(statement=query)

            # Logging stored procedure result
            if res:
                for r in res:
                    info(f"Return message: {r}")
        except Exception as e:
            error(f".... ERROR with executing query:  {e.__class__.__name__} - {e}")
            error(f".... QUERY: {query}")
            sys.exit(1)
        finally:
            self.__dispose()

    def upload_pii_data(self):
        """
        Upload PII data
        """
        info(".... START upload_pii_data.")
        self.__execute_query(query=self.pii_query)
        info(".... END upload_pii_data.")

    def delete_data(self):
        """
        Delete data from the table
        """
        info(".... START deleting data.")
        self.__execute_query(query=self.delete_data_query)
        info(".... END deleting data.")

    def update_mnpi_metadata(self):
        """
        Update timestamp data for MNPI
        as initially we do not have it
        """
        info(".... START update MNPI metadata.")
        self.__execute_query(query=self.mnpi_metadata_update_query)
        info(".... END update MNPI metadata.")

    def upload_mnpi_data(self):
        """
        Upload MNPI data
        """
        info(".... START upload_mnpi_data.")
        self.upload_to_snowflake()
        self.update_mnpi_metadata()
        info(".... END upload_mnpi_data.")

    def extract(self):
        """
        Routine to extract objects needed for tagging
        """
        info("START extract.")
        self.delete_data()
        self.upload_pii_data()
        self.upload_mnpi_data()
        info("END extract.")
