"""
This the transformation code for deduplication of data of postgres
"""

import logging
from datetime import datetime
from os import environ as env
from typing import Dict

from fire import Fire
from gitlabdata.orchestration_utils import query_executor, snowflake_engine_factory
from yaml import YAMLError, safe_load

file_name = "gitlab_deduplication/manifest_deduplication/t_gitlab_com_deduplication_table_manifest.yaml"


def build_table_name(
    table_prefix: str = None, table_name: str = None, table_suffix: str = None
) -> str:
    """The function is responsible for adding prefix and suffix to create table name"""

    res = ""
    if table_prefix:
        res += table_prefix

    if table_name:
        res += table_name

    if table_suffix:
        res += table_suffix

    return res


def create_table_name(table_prefix: str, table_name: str) -> tuple:
    """
    Prepare the backup table name and original table name
    as the table name passed in manifest i postgres table name not the snowflake table name
    """
    table_suffix = "_" + datetime.now().strftime("%Y%m%d")

    if table_prefix:
        bkp_table_name = build_table_name(
            table_prefix=table_prefix, table_name=table_name, table_suffix=table_suffix
        )
        original_table_name = build_table_name(
            table_prefix=table_prefix, table_name=table_name
        )
    else:
        bkp_table_name = build_table_name(
            table_name=table_name, table_suffix=table_suffix
        )
        original_table_name = build_table_name(table_name=table_name)
    return bkp_table_name, original_table_name


def create_backup_table(
    manifest_dict: dict,
    table_name: str,
) -> bool:
    """
    This function create clone of the table in the backup schema,
    if the backup has been taken today itself it will abort here.
    """

    raw_database = manifest_dict["raw_database"]
    backup_schema_name = manifest_dict["generic_info"]["backup_schema"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    table_prefix = manifest_dict["generic_info"]["table_prefix"]
    bkp_table_name, original_table_name = create_table_name(
        table_prefix=table_prefix, table_name=table_name
    )
    query_create_backup_table = f"CREATE TABLE IF NOT EXISTS {raw_database}.{backup_schema_name}.{bkp_table_name} CLONE {raw_database}.{raw_schema}.{original_table_name}; "

    logging.info(f"Backup table DDL : {query_create_backup_table}")
    backup_table = query_executor(snowflake_engine, query_create_backup_table)
    logging.info(backup_table)
    return True


def create_temp_table_ddl(manifest_dict: Dict, table_name: str):
    """This function is responsible to generate the temp table DDL for the passed table parameters"""
    raw_database = manifest_dict["raw_database"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    table_prefix = manifest_dict["generic_info"]["table_prefix"]
    bkp_table_name, original_table_name = create_table_name(
        table_prefix=table_prefix, table_name=table_name
    )
    backup_schema = manifest_dict["generic_info"]["backup_schema"]
    build_ddl_statement = f"""SELECT CONCAT('CREATE TABLE {raw_database}.{raw_schema}.{original_table_name}_temp\n AS (' ,'SELECT  \n',
                                    LISTAGG(
                                            CASE
                                            WHEN LOWER(column_name) = '_uploaded_at' THEN 'MIN( _uploaded_at) AS _uploaded_at'
                                            WHEN LOWER(column_name) = '_task_instance' THEN 'MIN(_task_instance)  AS _task_instance'
                                            ELSE column_name
                                            END ,',\n') WITHIN GROUP (ORDER BY ordinal_position ASC)
                                            , ' \n FROM {raw_database}.{backup_schema}.{bkp_table_name}' ,
                             '\n GROUP BY ALL);')
                                    from {raw_database}.information_schema.columns
                                    where LOWER(table_name) = '{original_table_name}';"""

    # Create the temporary table definition
    table_definition_to_create_table = query_executor(
        snowflake_engine, build_ddl_statement
    )
    logging.info(table_definition_to_create_table[0][0])
    return table_definition_to_create_table[0][0]


def create_temp_table(manifest_dict: Dict, table_name: str) -> None:
    """
    This function create the temporary table in the original schema.
    This temp table contains most recent unique rows.
    """

    table_definition = create_temp_table_ddl(manifest_dict, table_name)
    execute_create_temp_table = query_executor(snowflake_engine, table_definition)
    logging.info(execute_create_temp_table)


def create_swap_table_ddl(
    raw_database: str, raw_schema: str, temp_table_name: str, original_table_name: str
) -> str:
    """
    This function responsible for the creating the swap table DDL.
    """
    return f"ALTER TABLE {raw_database}.{raw_schema}.{temp_table_name} SWAP WITH {raw_database}.{raw_schema}.{original_table_name};"


def create_drop_table_ddl(
    raw_database: str, raw_schema: str, temp_table_name: str
) -> str:
    """
    The function return the drop ddl statement for the temp table.
    """
    return f"DROP TABLE {raw_database}.{raw_schema}.{temp_table_name};"


def swap_and_drop_temp_table(manifest_dict: Dict, table_name: str):
    """
    The function swaps the table and drop the temp table created
    """
    raw_database = manifest_dict["raw_database"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    table_prefix = manifest_dict["generic_info"]["table_prefix"]
    _, original_table_name = create_table_name(
        table_prefix=table_prefix, table_name=table_name
    )

    # Swap the table name with main table.
    temp_table_name = f"{original_table_name}_temp"
    swap_query = create_swap_table_ddl(
        raw_database=raw_database,
        raw_schema=raw_schema,
        temp_table_name=temp_table_name,
        original_table_name=original_table_name,
    )

    # Drop the temp table in tap_postgres schema
    drop_query = create_drop_table_ddl(
        raw_database=raw_database,
        raw_schema=raw_schema,
        temp_table_name=temp_table_name,
    )

    logging.info(f"Swap query:{swap_query}")
    swap_table = query_executor(snowflake_engine, swap_query)
    logging.info(swap_table)

    if swap_table:
        logging.info(f"Drop query:{drop_query}")
        drop_temp_table = query_executor(snowflake_engine, drop_query)
        logging.info(drop_temp_table)


def get_yaml_file(path: str):
    """
    Get all the report name for which tasks for loading
    needs to be created
    """

    with open(file=path, mode="r", encoding="utf-8") as file:
        try:
            loaded_file = safe_load(stream=file)
            return loaded_file
        except YAMLError as exc:
            logging.error(f"Issue with the yaml file: {exc}")
            raise


def main(table_name: str) -> None:
    """
    Read table name FROM manifest file and decide if the table exist in the database. Check if the advance metadata column `_task_instance`
    is present in the table.
    Check for backup schema is present in snowflake.
    Check for old backup table in snowflake and if present check if the creation date is older than 15 days. If yes drop backup table if not create a new backup table.
    Once the backup is complete, create  table with deduplicate records.
    Swap the table with original table.
    Drop the swapped temp table.
    """
    # Process the manifest
    logging.info(f"Proceeding with table {table_name} for deduplication")
    manifest_dict = get_yaml_file(path=file_name)

    # Update database name to the manifest for in case of MR branch.
    raw_database = f'"{env.copy()["SNOWFLAKE_LOAD_DATABASE"]}"'
    manifest_dict.update({"raw_database": raw_database})

    # Create backup table
    create_clone_table = create_backup_table(manifest_dict, table_name)

    # validate if backup created successfully then create the temp table.
    if create_clone_table:
        create_temp_table(manifest_dict, table_name)
        swap_and_drop_temp_table(manifest_dict, table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    Fire({"deduplication": main})
