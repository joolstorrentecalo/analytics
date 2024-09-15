"""
    API class file for zuora data query
"""

import json
import logging
import time
from datetime import datetime
from io import StringIO
from logging import info
from os import environ as env
from typing import Dict

import pandas as pd
import requests
from gitlabdata.orchestration_utils import snowflake_engine_factory


class ZuoraQueriesAPI:
    """_summary_
    Main class file to authentication ,
    running query
    generating date range
    """

    def __init__(self, config_dict: Dict):
        """

        :param config_dict:
        :type config_dict:
        """
        zuora_api_client_id = env["ZUORA_DEV_API_CLIENT_ID"]
        zuora_api_client_secret = env["ZUORA_DEV_API_CLIENT_SECRET"]
        self.base_url = "https://rest.test.zuora.com"

        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

        zuora_token = self.authenticate_zuora(
            zuora_api_client_id, zuora_api_client_secret
        )

        self.request_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {zuora_token}",
        }

    def authenticate_zuora(
        self, zuora_api_client_id: str, zuora_api_client_secret: str
    ) -> str:
        """
        Written to encapsulate Zuora's authentication functionality
        :param zuora_api_client_id:
        :type zuora_api_client_id:
        :param zuora_api_client_secret:
        :type zuora_api_client_secret:
        :return:
        :rtype:
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data_auth = {
            "client_id": zuora_api_client_id,
            "client_secret": zuora_api_client_secret,
            "grant_type": "client_credentials",
        }
        auth_url = f"{self.base_url}/oauth/token"
        response = requests.post(auth_url, headers=headers, data=data_auth, timeout=60)
        if response.ok:
            info("Successful auth")
            return response.json()["access_token"]

        logging.error(response.status_code)
        logging.error(response.json())
        raise ConnectionError("COULD NOT AUTHENTICATE")

    def request_data_query_data(self, query_string: str) -> str:
        """

        :param query_string: Written in ZQL (check Docs to make changes),
        :param query_type:
        :return:
        """
        api_url = f"{self.base_url}/query/jobs"

        payload = dict(
            compression="NONE",
            output=dict(target="S3"),
            outputFormat="CSV",
            query=query_string,
        )

        response = requests.post(
            api_url, headers=self.request_headers, data=json.dumps(payload), timeout=60
        )

        if response.status_code == 200:
            logging.info("Successful request")
            return response.json().get("data").get("id")

        logging.error(response.json)
        raise ConnectionError("Error requesting job")

    def get_job_data(self, job_id: str) -> Dict:
        """

        :param job_id:
        :type job_id:
        :return:
        :rtype:
        """
        api_url = f"{self.base_url}/query/jobs"
        response = requests.get(api_url, headers=self.request_headers, timeout=300)
        data = response.json()
        job = [j for j in data.get("data") if j.get("id") == job_id]
        if len(job) > 0:
            return job[0]

        raise ReferenceError("Job not found")

    def get_data_query_file(self, job_id: str, wait_time: int = 30) -> pd.DataFrame:
        """

        :param job_id:
        :type job_id:
        :param wait_time:
        :type wait_time:
        :return:
        :rtype:
        """
        job = self.get_job_data(job_id)

        job_status = job.get("queryStatus")

        info(f" Job status {job_status}")
        if job_status in ["failed", "cancelled"]:
            raise ValueError(f"Job {job_status}")

        while job_status in ["accepted", "in_progress"]:
            # If job is not yet available, wait for 30 seconds.
            time.sleep(wait_time)

            job = self.get_job_data(job_id)

            job_status = job.get("queryStatus")
            info(f"Waiting for report to complete, current status {job_status}")

        if job_status == "completed":
            info("File ready")
            file_url = job["dataFile"]
            response = requests.get(url=file_url, timeout=300)

            df = pd.read_csv(StringIO(response.text))
            info("File downloaded")
            return df

        job = self.get_job_data(job_id)
        job_status = job.get("queryStatus")
        raise ValueError(f"The job has failed or has been killed: {job_status}")

    def get_date_interval_list(self, start_date, end_date):
        """Generate time interval range with end_date of one interval overlapping the other, and extract value from dataframe
        and generate list of interval."""
        date_list = pd.DataFrame(
            {
                "Date": pd.interval_range(
                    pd.Timestamp(start_date),
                    pd.Timestamp(end_date),
                    periods=10,
                    closed="neither",
                )
            }
        )

        convert_interval_to_list = []
        for _, row in date_list.iterrows():
            convert_interval_to_list.append(str(row._get_value("Date")))

        return convert_interval_to_list

    def get_start_end_date_value(self, date_interval_list: str) -> tuple:
        """
        Take a string value separated by comma, first it split it and make it a list and then
        replace the open and close bracket to form a proper start date and end date tuple.
        """
        split_date_list = date_interval_list.split(sep=",")
        start_date_value = split_date_list[0].replace("(", "").replace(")", "").strip()
        end_date_value = split_date_list[1].replace("(", "").replace(")", "").strip()
        return start_date_value, end_date_value

    def get_datetime_format(self, date_value: str, length_start_date: int) -> str:
        """
        Check if the length of the date is greater than the standard start date defined, then it generated date
        range is having extra number over seconds hence it needs to be stripped to meet the date format of the API call.
        """
        if len(date_value) > length_start_date:
            return (
                datetime.strptime(
                    date_value[: -(len(date_value) - length_start_date)],
                    "%Y-%m-%d %H:%M:%S",
                )
            ).strftime("%Y-%m-%d %H:%M:%S")

        return (datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    def date_range(self, start_date: str = None):
        """
        This function will return start_date and end_date for querying the zuora revenue BI view or table_name.
        If start_date is not passed then it sets itself to zuora default start_date.
        end_date is always set to now.
        """
        if not start_date:
            start_date_default = "2019-07-25 00:00:01"

        start_date = pd.to_datetime(start_date_default)
        end_date = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # Get length of fixed column for stripping extra nanoseconds downstream.
        length_start_date = len(str(start_date))
        final_date_list = []
        date_interval_list = self.get_date_interval_list(start_date, end_date)
        for date_list in date_interval_list:
            start_date_value, end_date_value = self.get_start_end_date_value(date_list)

            date_range_dict = {
                "start_date": self.get_datetime_format(
                    start_date_value, length_start_date
                ),
                "end_date": self.get_datetime_format(end_date_value, length_start_date),
            }
            final_date_list.append(date_range_dict)

        return final_date_list
