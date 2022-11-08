from os import environ as env

from pandas import DataFrame
from big_query_client import BigQueryClient

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)

config_dict = env.copy()


def get_pmg_reporting_data_query(start_date: str, end_date: str) -> str:
    return (
        f"""
        SELECT 
          date,
          utm_medium,
          utm_source,
          utm_campaign,
          campaign_code,
          geo,
          targeting,
          ad_unit,
          br_nb,
          match_unit,
          content,
          team,
          budget,
          sales_segment,
          data_source,
          impressions,
          clicks,
          conversions,
          cost,
          ga_conversions,
          sends, 
          opens, 
          inquiries, 
          mqls, 
          linear_sao, 
          campaign_code_type, 
          content_type 
        FROM 
          `pmg-datawarehouse.gitlab_analytics.cross_channel`
          WHERE date >= '{end_date}' and date < '{start_date}'
        """
    )


def write_date_json(date: str, df: DataFrame) -> str:
    Just here so we can log in the list comprehension
    file_name = fpmg_reporting_data_{date}.json
    print(fWriting file {file_name})

    df.to_json(file_name, orient=records, date_format=iso)

    print(f{file_name} written)

    return file_name


if __name__ == __main__:

    bq = BigQueryClient()

    # Substringing cause their only needed for string operations in the next function
    start_time = config_dict[START_TIME][0:10]
    end_time = config_dict[END_TIME][0:10]

    snowflake_engine = snowflake_engine_factory(config_dict, LOADER)

    sql_statement = get_pmg_reporting_data_query(start_time, end_time)
    # Groups by date so we can create a file for each day
    df_by_date = bq.get_dataframe_from_sql(sql_statement).groupby(date)

    written_files = [write_date_json(date, df) for date, df in df_by_date]

    [
        snowflake_stage_load_copy_remove(
            file_name, pmg.pmg_load, pmg.paid_digital, snowflake_engine
        )
        for file_name in written_files
    ]
