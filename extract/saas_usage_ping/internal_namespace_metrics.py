"""
Routines for Internal Namespace Metrics
"""
from logging import info
from typing import Any, Dict, Optional

from utils import Utils


class SQLGenerate:
    """
    This class is responsible for generating SQL statements based on provided metrics.
    It has a constructor to initialize the table name and a utility object.
    It has three methods: get_time_frame, get_event_list, and transform.
    The get_time_frame method returns a
    """

    def __init__(self):
        """
        Initialize the SQLGenerate class with the table name and utility object.
        """
        self.table_name = "prod.common_mart_product.mart_behavior_structured_event_service_ping_metrics"
        self.util = Utils()

    @staticmethod
    def get_time_frame(time_frame: Optional[Dict[Any, Any]] = None) -> str:

        """
        Get time frame for specific SQL
        """
        res = ""

        if time_frame == "7d":
            res = "AND behavior_at BETWEEN DATEADD(DAY, -7, between_end_date) AND between_end_date "
        if time_frame == "28d":
            res = "AND behavior_at BETWEEN DATEADD(DAY, -28, between_end_date) AND between_end_date "
        return res

    def get_event_list(self, metrics: dict) -> str:
        """
        Get event list for specific SQL
        """
        events_list = []
        events, options = metrics.get("events"), metrics.get("options")
        res = ""

        if events:
            events_list.extend([event.get("name") for event in events])

        if options:
            events_list.extend(options)

        if events_list:
            if len(events_list) == 1:
                res += f"AND redis_event_name = {self.util.quoted(events_list[0])} "
            else:
                res += f"AND redis_event_name IN ({','.join([self.util.quoted(event) for event in events_list])}) "
        return res

    def transform(self, metrics: dict) -> str:
        """
        Transform YML definition to SQL statement and template
        """

        # Generate SELECT part with columns
        res = (
            f"SELECT "
            f"ultimate_parent_namespace_id AS id, "
            f"ultimate_parent_namespace_id AS namespace_ultimate_parent_id, "
            f"COUNT(DISTINCT gsc_pseudonymized_user_id) AS counter_value "
            f"FROM {self.table_name} "
        )
        # Generate WHERE clause for metrics
        res += f"WHERE metrics_path={self.util.quoted(metrics.get('key_path'))} "
        # Generate list of event, if any
        res += self.get_event_list(metrics=metrics)
        # Generate time frame, if any
        res += self.get_time_frame(time_frame=metrics.get("time_frame"))

        res += "GROUP BY ALL;"
        return res


class InternalNamespaceMetrics:
    """
    Routines for internal namespace metrics
    """

    def __init__(self):
        """
        Initialize the InternalNamespaceMetrics class.

        This method sets up the initial state of the InternalNamespaceMetrics object.
        It initializes the following attributes:
        - yml_file_name: The name of the YAML file containing metric definitions.
        - sql_file_name: The name of the JSON file to
        """
        self.yml_file_name = "internal_namespace_metrics_definition.yml"
        self.sql_file_name = "internal_namespace_metrics_queries.json"
        self.util = Utils()
        self.util.headers = {}

    @staticmethod
    def get_sql_metrics_definition(metric: dict) -> dict:
        """
        Generate SQL template for internal namespace metrics
        """
        template = {}
        sql = SQLGenerate()

        template["counter_name"] = metric.get("key_path")
        template["counter_query"] = sql.transform(metrics=metric)
        template["time_window_query"] = metric.get("time_frame") in ("7d", "28d")
        template["level"] = "namespace"

        return template

    def generate_sql_metrics(self, metrics_data: dict) -> list:
        """
        Generate SQL metrics for internal namespace metrics.
        """
        res = []
        for metric in metrics_data:
            if metric.get("status") == "active":
                res.append(self.get_sql_metrics_definition(metric=metric))

        return res

    def transform_yml_json(self, yml_file: str, data) -> dict:
        """
        transform yml to json file
        """
        try:
            self.util.save_to_yml_file(file_name=yml_file, data=data)
            return self.util.load_from_yml_file(file_name=yml_file)
        finally:
            self.util.delete_file(file_name=yml_file)

    def generate(self):
        """
        1. Get metrics definition from API
        2. Transform YML to JSON
        3. Transform JSON to SQL
        4. Save SQL to file
        """
        info("Start generating internal namespace metrics")
        url = "https://gitlab.com/api/v4/usage_data/metric_definitions"
        metrics_raw = self.util.get_response(url=url)

        metrics_prepared = self.transform_yml_json(
            yml_file=self.yml_file_name, data=metrics_raw
        )
        metrics = self.generate_sql_metrics(metrics_data=metrics_prepared)
        self.util.save_to_json_file(file_name=self.sql_file_name, json_data=metrics)

        info("End generating internal namespace metrics")


if __name__ == "__main__":
    internal_namespace_metrics = InternalNamespaceMetrics()
    internal_namespace_metrics.generate()
