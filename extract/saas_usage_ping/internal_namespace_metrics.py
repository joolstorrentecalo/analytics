import datetime
import math
import os
import sys
from logging import basicConfig, info

from engine_factory import EngineFactory
from fire import Fire
from utils import Utils


class InternalNamespaceMetrics:
    def __init__(
        self,
        ping_date=None,
        chunk_no=0,
        number_of_tasks=0,
        internal_namespace_metrics_filter=None,
    ):
        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        self.start_date_28 = self.end_date - datetime.timedelta(28)

        if internal_namespace_metrics_filter is not None:
            self.metrics_backfill_filter = internal_namespace_metrics_filter
        else:
            self.metrics_backfill_filter = []

        # chunk_no = 0 - internal_namespace_metrics back filling (no chunks)
        # chunk_no > 0 - load internal_namespace_metrics in chunks
        self.chunk_no = chunk_no
        self.number_of_tasks = number_of_tasks

        self.table_name = "gitlab_internal_namespace"

        self.engine_factory = EngineFactory()
        self.utils = Utils()

        self.SQL_INSERT_PART = (
            "INSERT INTO "
            f"{self.engine_factory.schema_name}.{self.table_name}"
            "(id, "
            "namespace_ultimate_parent_id, "
            "counter_value, "
            "ping_name, "
            "level, "
            "query_ran, "
            "error, "
            "ping_date, "
            "_uploaded_at) "
        )

    def generate(self):
        # download_file from RESTful API endpoint
        # transform_yml_json
        # generate_sql INSERT + SELECT
        # save_sql_to_json
        pass

    def calculate(self):
        # load_json
        # decouple SQLs in chunks
        # execute SQL
        pass


if __name__ == "__main__":
    pass
