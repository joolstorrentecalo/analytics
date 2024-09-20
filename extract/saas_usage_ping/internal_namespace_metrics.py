"""
Routines for Internal Namespace Metrics
"""
import json
from logging import info

from utils import Utils


class SQLGenerate:
    def __init__(self):
        self.table_name = "prod.common_mart_product.mart_behavior_structured_event_service_ping_metrics"
        self.util = Utils()

    def get_time_frame(self, time_frame: str) -> str:
        """
        Get time frame for specific SQL
        """
        res = ""

        if time_frame == "7d":
            res = "AND behavior_at BETWEEN DATEADD(DAY, -7, between_end_date) AND between_end_date "
        if time_frame == "28d":
            res = "AND behavior_at BETWEEN DATEADD(DAY, -28, between_end_date) AND between_end_date "
        return res

    def get_event_list(self, metrics: list) -> str:
        """
        Get event list for specific SQL
        """
        events_list = []
        res = ""

        if metrics.get("events"):
            events_list.extend([x.get("name") for x in metrics.get("events")])

        if metrics.get("options"):
            events_list.extend(metrics.get("options"))

        if events_list:
            if len(events_list) == 1:
                res += f"AND redis_event_name = {self.util.quoted(events_list[0])} "
            else:
                res += f"AND redis_event_name IN ({','.join([self.util.quoted(event) for event in events_list])}) "
        return res

    def transform(self, metrics: list) -> str:
        """
        Transform YML definition to SQL statement and template
        """

        res = f"SELECT ultimate_parent_namespace_id AS id, ultimate_parent_namespace_id AS namespace_ultimate_parent_id, COUNT(DISTINCT gsc_pseudonymized_user_id) AS counter_value FROM {self.table_name} "
        #  res += F"WHERE metrics_path='{metrics.get("key_path")}' "

        res += self.get_event_list(metrics=metrics)

        res += self.get_time_frame(time_frame=metrics.get("time_frame"))

        res += "GROUP BY ALL;"
        return res


class InternalNamespaceMetrics:
    def __init__(self):
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
        template["time_window_query"] = (
            True if metric.get("time_frame") in ("7d", "28d") else False
        )
        template["level"] = "namespace"

        return template

    def generate_sql_metrics(self, metrics_data: json):
        res = []
        for metric in metrics_data:
            if metric.get("status") == "active":
                res.append(self.get_sql_metrics_definition(metric=metric))

        return res

    def transform_yml_json(self, yml_file: str, data) -> json:
        """
        transform yml to json file
        """
        try:
            self.util.save_to_yml_file(file_name=yml_file, data=data)
            return self.util.load_from_yml_file(file_name=yml_file)
        finally:
            self.util.delete_file(file_name=yml_file)

    def generate(self):
        info("Start generating internal namespace metrics")
        url = "https://gitlab.com/api/v4/usage_data/metric_definitions"
        metrics_raw = self.util.get_response(url=url)

        metrics_prepared = self.transform_yml_json(
            yml_file=self.yml_file_name, data=metrics_raw
        )
        metrics = self.generate_sql_metrics(metrics_data=metrics_prepared)
        self.util.save_to_json_file(file_name=self.sql_file_name, json_data=metrics)

        info("End generating internal namespace metrics")

    def calculate(self):
        # load_json
        # decouple SQLs in chunks
        # execute SQL
        pass


if __name__ == "__main__":
    internalnamespacemetrics = InternalNamespaceMetrics()
    internalnamespacemetrics.generate()
