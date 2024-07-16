import logging
import yaml
from os import environ as env
from typing import List

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_list_of_dbs_to_keep(yaml_path="analytics/permissions/snowflake/roles.yml"):
    with open(yaml_path, "r") as yaml_content:
        role_dict = yaml.load(yaml_content, Loader=yaml.FullLoader)
        return [list(db.keys())[0].lower() for db in role_dict["databases"]]


def get_list_of_stale_dev_tables(engine: Engine) -> List[str]:
    """
    Get a list of tables in development tables that are beyond the retention period defined in dbt_project.yml.
    This will make sure sensitive data is not hanging around.
    """

    query = """
    SELECT
      table_catalog,
      table_schema,
      table_name,
    FROM prod.data_quality.stale_dev_db_tables
    """

    try:
        logging.info("Getting list of stale dev tables...")
        connection = engine.connect()
        stale_tables = [row for row in connection.execute(query).fetchall()]
    except:
        logging.info("Failed to get list of stale tables...")
    finally:
        connection.close()
        engine.dispose()

    return stale_tables


def get_list_of_clones(engine: Engine) -> List[str]:
    """
    Get a list of all databases besides the ones to keep.
    This will delete clones for MRs that are older than at least 7 days, so users may need to rerun the review job.
    """

    query = """
    SELECT DATABASE_NAME as database_name
    FROM INFORMATION_SCHEMA.DATABASES
    WHERE LAST_ALTERED < CURRENT_DATE -14
    """

    try:
        logging.info("Getting list of databases...")
        connection = engine.connect()
        databases = [row[0] for row in connection.execute(query).fetchall()]
    except Exception as exc:
        logging.info("Failed to get list of databases...")
        logging.info(exc)
    finally:
        connection.close()
        engine.dispose()

    dbs_to_keep = get_list_of_dbs_to_keep()

    return [database for database in databases if database.lower() not in dbs_to_keep]


def drop_databases() -> None:
    """
    Drop each of the databases for the clones that exist.
    """

    logging.info("Preparing to drop databases...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    logging.info("Creating list of clones...")
    databases = get_list_of_clones(engine)

    for database in databases:
        drop_query = f"""DROP DATABASE "{database}";"""
        try:
            connection = engine.connect()
            connection.execute(drop_query)
        except:
            logging.info(f"Failed to drop database: {database}")
        finally:
            connection.close()
            engine.dispose()


def drop_stale_dev_tables() -> None:
    """
    Drop each of the stale tables
    """

    logging.info("Preparing to drop stale dev tables...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    stale_tables = get_list_of_stale_dev_tables(engine)
    logging.info(f"Dropping {len(stale_tables)} stale tables...")

    try:
        connection = engine.connect()

        for database, schema, table in stale_tables:
            fully_qualified_table_name = f'"{database}"."{schema}"."{table}"'
            drop_query = f"DROP TABLE {fully_qualified_table_name};"
            try:
                logging.info(f"Running: {drop_query}")
                connection.execute(drop_query)
            except:
                logging.info(f"Failed to drop table: {fully_qualified_table_name}")
    except:
        logging.info(f"Failed to connect to snowflake")
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(
        {
            "drop_stale_dev_tables": drop_stale_dev_tables,
            "drop_databases": drop_databases,
        }
    )
    logging.info("Complete.")
