"""
Module returning a csv with downstream dbt dependencies and
data science exposures for a list of model provided by the user.
"""

import json

import pandas as pd
from gitlabdata.orchestration_utils import query_dataframe
from gitlabdata.orchestration_utils import data_science_engine_factory


def run():
    """
    Run the dependency check
    """
    # file_name = get_file_name()

    # model_names = get_model_names(file_name=file_name)
    # generate_results(model_names=model_names)

    with open("./target/manifest.json", "r") as read_manifest:
        manifest_data = json.load(read_manifest)

    with open("./target/run_results.json", "r") as read_file:
        result_data = json.load(read_file)

    results = []

    for res in result_data.get("results"):

        node_manifest = manifest_data.get("nodes").get(res.get("unique_id"))
        node_config = node_manifest.get("config")
        node_id = res.get("unique_id")
        invocation_command = result_data.get("args").get("invocation_command")
        invocation_is_full_refresh = (
            True if invocation_command.find("--full-refresh") > 0 else False
        )
        run_started_at = result_data.get("metadata").get("generated_at")
        rows_affected = res.get("adapter_response").get("rows_affected")
        execution_time = res.get("execution_time")
        materialized = node_config.get("materialized")
        full_refresh = (
            invocation_is_full_refresh
            if node_config.get("full_refresh") is None
            else node_config.get("full_refresh")
        )
        node_data = {
            "node_id": node_id,
            "run_started_at": run_started_at,
            "rows_affected": rows_affected,
            "execution_time": execution_time,
            "materialized": materialized,
            "full_refresh": full_refresh,
        }
        results.append(node_data)

    node_data_df = pd.DataFrame(results)
    # print(node_data_df.dtypes)
    # for node in manifest_data.get("nodes").get(res.get("unique_id")):
    #    print(node.get("unique_id").value)

    # print(result_data)

    query_engine = data_science_engine_factory()

    query = f"""
      select
          node_id,
          run_started_at,
          rows_affected,
          total_node_runtime AS execution_time,              
          materialization AS materialized,
          was_full_refresh AS full_refresh
        from prod.workspace_data.fct_dbt__model_executions
        WHERE run_started_at > DATEADD('day',-35,CURRENT_DATE)
        AND run_started_at < DATE_TRUNC('day',CURRENT_DATE)
        AND status = 'success'
        --AND node_id ='{node_id}'
        --AND was_full_refresh = {full_refresh}
        QUALIFY row_number() OVER (PARTITION BY node_id, was_full_refresh ORDER BY fct_dbt__model_executions.run_started_at DESC) = 1
    """

    query_result = query_dataframe(query_engine, query)
    # print(query_result.dtypes)

    node_result_df = node_data_df.set_index(["node_id", "full_refresh"]).join(
        query_result.set_index(["node_id", "full_refresh"]),
        lsuffix="_dev",
        rsuffix="_prod",
        how="inner",
    )
    print(node_result_df)

    node_result_df.to_csv(
        "__performance_check.csv",
        index=True,
        encoding="utf-8",
    )

    # dbt = dbtRunner()

    # cli_args = ["run", "--select", "data_type_mock_table"]

    # run the command for models
    # res_models: dbtRunnerResult = dbt.invoke(cli_args)

    # print(res_models)


if __name__ == "__main__":
    run()
