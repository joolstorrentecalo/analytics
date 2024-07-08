"""
Create a connection to Snowflake with the appropriate user/role
"""

from logging import info
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import text
from typing import Any, List, Tuple


class SnowflakeConnection:
    """Class to connect to Snowflake"""

    def __init__(
        self,
        user: str,
        password: str,
        account: str,
        role: str,
        warehouse: str,
        is_test_run: bool = True,
    ):
        self.is_test_run = is_test_run

        # only create engine if NOT test run
        if not self.is_test_run:
            self.engine = create_engine(
                URL(
                    user=user,
                    password=password,
                    account=account,
                    role=role,  # needs to be passed in, can be securityadmin/sysadmin
                    warehouse=warehouse,
                )
            )

    def query_executor(self, query: str, query_params: dict = {}) -> List[Tuple[Any]]:
        """
        Execute DB queries safely.
        """

        with self.engine.connect() as connection:
            query_text = text(query)
            results = connection.execute(query_text, query_params).fetchall()
        return results

    def run_sql_statement(self, sql_statement: str, query_params: dict = {}):
        """run individual sql statement"""
        action = "Printing" if self.is_test_run else "Running"
        info(f"{action} sql_statement: {sql_statement}")
        if self.is_test_run:
            return

        query_result = SnowflakeConnection.query_executor(
            self.engine, sql_statement, query_params
        )
        info(f"query_result: {query_result}")
        return query_result

    def run_sql_statements(self, sql_statements: list, query_params: dict):
        """process all sql statements"""
        for sql_statement in sql_statements:
            self.run_sql_statement(sql_statement, query_params)

    def dispose_engine(self):
        if not self.is_test_run:
            self.engine.dispose()
