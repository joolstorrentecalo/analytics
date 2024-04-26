""" Main class for zuora revenue api report"""

import logging
import sys
import json
import subprocess
from typing import Dict
from datetime import datetime, timedelta
import os
import requests
import yaml
import pandas as pd


class ZuoraRevProAPI:
    """Zuora class to include all functionality"""

    def __init__(self, config_dict: Dict):
        self.headers = config_dict["headers"]
        self.authenticate_url_zuora_revpro = config_dict[
            "authenticate_url_zuora_revpro"
        ]
        self.zuora_fetch_data_url = config_dict["zuora_download_report_url"]
        self.bucket_name = config_dict["bucket_name"]
        self.zuora_fetch_report_list_url = config_dict["zuora_fetch_report_list_url"]
        logging.basicConfig(
            filename="zuora_report_extract.log",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=20,
        )
        self.logger = logging.getLogger(__name__)
        zuora_token = self.get_auth_token()
        self.request_headers = {
            "token": f"{zuora_token}",
        }
        self.zuora_report_output_directory = (
            config_dict["zuora_report_home"] + "report_output/"
        )

    def get_auth_token(self) -> str:
        """
        This function receives url and header information and generate authentication token in response.
        If we don't receive the response status code  200 then exit the program.
        """
        response = requests.post(
            self.authenticate_url_zuora_revpro, headers=self.headers, timeout=20
        )
        if response.status_code == 200:
            self.logger.info("Authentication token generated successfully")
            authToken = response.headers["Revpro-Token"]
            return authToken
        else:
            raise ConnectionError(
                f"HTTP {response.status_code} - {response.reason}, Message {response.text}"
            )

    @staticmethod
    def get_extract_date(extract_date: str = None) -> str:
        """
        This function provide the date for which we need to look for report.Reports can be downloaded only for
        48 hours from the time it runs.
        If todays  data has to be loaded then pass that as parameter else it will download data for current_date - 1.
        Zuora consume date in format `DD-MMM-YYYY` example `16-JAN-2023`
        """

        if extract_date is None:
            yesterday = datetime.now() - timedelta(1)
            extract_date = datetime.strftime(yesterday, "%d-%b-%Y")

        return extract_date

    @staticmethod
    def get_report_list_df(report_list: Dict) -> pd.DataFrame:
        """Return the data frame of list of reports for the particular day"""
        return pd.DataFrame(report_list)

    def get_report_list(self, extract_date: str) -> pd.DataFrame:
        """Hit the URL to fetch all the report executed in the particular date.
        It finally return the data frame of report list"""

        get_report_list_url = f"{self.zuora_fetch_report_list_url}{extract_date}"
        report_list = requests.get(
            get_report_list_url, headers=self.request_headers, timeout=20
        )

        if report_list.status_code == 200:
            zuora_report_list = json.loads(report_list.text)
        else:
            self.logger.error(
                f"HTTP {report_list.status_code} - {report_list.reason}, Message {report_list.text}"
            )
            sys.exit(1)

        return self.get_report_list_df(zuora_report_list.get("Result"))

    @staticmethod
    def get_requested_report(
        zuora_report_api_list: str = "zuora_report_api_list.yml",
    ) -> Dict:
        """Open the yml file to get the list of reports which needs to be downloaded."""

        with open(zuora_report_api_list, encoding="utf-8") as file:
            report_list_to_download = yaml.load(file, Loader=yaml.FullLoader)
        return report_list_to_download

    def get_report_list_key(self, zuora_requested_report_list) -> list:
        """Extract list of report name to iterate from the manifest file for which extraction needs to be done"""
        report_list_key = list(zuora_requested_report_list["report_list"].keys())
        self.logger.info(
            f"List of report which needs to be downloaded from zuora system:{report_list_key}"
        )
        return report_list_key

    @staticmethod
    def get_report_list_dict(zuora_report_list_to_download, report: str) -> Dict:
        """Read manifest file for all required values"""
        report_list_dict = {}
        report_list_dict["category"] = zuora_report_list_to_download["report_list"][
            report
        ]["category"]
        report_list_dict["rep_name"] = zuora_report_list_to_download["report_list"][
            report
        ]["rep_name"]
        report_list_dict["layout_name"] = zuora_report_list_to_download["report_list"][
            report
        ]["layout_name"]
        report_list_dict["rep_desc"] = zuora_report_list_to_download["report_list"][
            report
        ]["rep_desc"]
        report_list_dict["output_file_name"] = zuora_report_list_to_download[
            "report_list"
        ][report]["output_file_name"]
        report_list_dict["static_rows_column_header"] = zuora_report_list_to_download[
            "report_list"
        ][report]["static_rows_column_header"]

        return report_list_dict

    @staticmethod
    def get_csv_filename(file_name: str) -> str:
        """If the filename has exstention other than csv rename it csv"""
        split_file_name = file_name.split(".", 1)
        if split_file_name[1] != "csv":
            file_name = split_file_name[0] + ".csv"
        return file_name

    def get_filename_without_csv(self, file_name: str) -> str:
        """Remove the suffix of the file."""
        split_file_name = file_name.split(".", 1)
        return split_file_name[0]

    def get_file_size_from_url(
        self, download_report_url: str, csv_file_name: str
    ) -> None:
        """Get the file size not a must have condition but for logging purpose."""
        report_file = requests.get(
            download_report_url, headers=self.request_headers, timeout=20
        )
        self.logger.info(
            f"size of file {csv_file_name} is :{report_file.headers.get('content-length')}"
        )

    def zuora_download_file(
        self, file_name: str, output_file_name, report_date_formatted: str
    ) -> None:
        """This function get the file size and download the file."""
        csv_file_name = self.get_csv_filename(file_name)
        filename_without_csv = self.get_filename_without_csv(file_name)
        download_report_url = f"{self.zuora_fetch_data_url}{csv_file_name}"
        self.get_file_size_from_url(download_report_url, csv_file_name)

        report_file = requests.get(
            download_report_url, headers={"token": self.get_auth_token()}, timeout=20
        )
        full_file_name_path = (
            self.zuora_report_output_directory
            + "/"
            + report_date_formatted
            + "/"
            + output_file_name
            + "_"
            + filename_without_csv
            + ".csv"
        )
        os.makedirs(os.path.dirname(full_file_name_path), exist_ok=True)
        if report_file.status_code == 200:
            with open(f"{full_file_name_path}", "w+", encoding="utf-8") as file:
                file.write(report_file.text)
        else:
            self.logger.error(
                f"HTTP {report_file.status_code} - {report_file.reason}, Message {report_file.text}"
            )
            sys.exit(1)

    def zuora_download_report(
        self,
        zuora_report_list_df: pd.DataFrame,
        zuora_report_list_to_download: list,
        report_date: str,
    ) -> None:
        """This function is responsible to get the list of reports to download and check if the report exist in the api call for that date.
        If present then download the report to the output directory."""
        requested_report_list = self.get_report_list_key(zuora_report_list_to_download)
        self.logger.info(f"List of reports to download: {requested_report_list}")
        report_date_formatted = report_date.replace("-", "_")
        # Iterate over each report which is required to be downloaded.
        for report in requested_report_list:
            self.logger.info(f"Looking for report : {report}")
            report_list_dict = self.get_report_list_dict(
                zuora_report_list_to_download, report
            )
            file_name = zuora_report_list_df.loc[
                (zuora_report_list_df["category"] == report_list_dict.get("category"))
                & (
                    zuora_report_list_df["layout_name"]
                    == report_list_dict.get("layout_name")
                )
                & (zuora_report_list_df["rep_name"] == report_list_dict.get("rep_name"))
                & (
                    zuora_report_list_df["rep_desc"] == report_list_dict.get("rep_desc")
                ),
                ["file_name"],
            ]
            list_of_file = [i for s in file_name.values for i in s]
            for file in list_of_file:
                self.logger.info(f"downloading file : {file}")
                output_file = report_list_dict.get("output_file_name")
                self.zuora_download_file(file, output_file, report_date_formatted)

    @staticmethod
    def get_all_downloaded_file_list(file_directory: str) -> list:
        """Get list of all the file ending with .csv from the directory"""
        res = []
        # Iterate directory
        for file in os.listdir(file_directory):
            # check only text files
            if file.endswith(".csv"):
                res.append(file)
        return res

    @staticmethod
    def get_file_list_to_upload(file_directory: str) -> list:
        """Get list of all the file post split up i.e. all header and body of the report to get uploaded to GCS bucket"""
        res = []
        # Iterate directory
        for file in os.listdir(file_directory):
            # check only text files
            if file.startswith("header") or file.startswith("body"):
                res.append(file)
        return res

    @staticmethod
    def split_file(file_directory: str, file: str, report_static_column_list) -> None:
        """Split file into 2 parts i.e. one with header information and other the content of the file. Also remove the file after split up"""
        file_name = file
        header_rows = int(report_static_column_list)
        body_rows = int(report_static_column_list) + 1
        with open(f"{file_directory}{file_name}", "r", encoding="utf-8") as filedata:
            linesList = filedata.readlines()

        with open(
            f"{file_directory}header_{file_name}", "w", encoding="utf-8"
        ) as file_header:
            for line in linesList[:header_rows]:
                file_header.write(line)

        with open(
            f"{file_directory}body_{file_name}", "w", encoding="utf-8"
        ) as file_body:
            for body_line in linesList[body_rows:]:
                file_body.write(body_line)

        os.remove(f"{file_directory}{file}")

    def upload_delete(
        self, file_directory: str, file_name: str, report_date: str
    ) -> None:
        """Upload the file to GCS. Pass 3 parameters source file directory to pick up file, File Name to upload and report date on which the report was executed.
        Also delete the file from local
        """

        command_upload_to_gcs = f"gsutil cp {file_directory}/{file_name} gs://{self.bucket_name}/RAW_DB/zuora_revenue_report/staging/{report_date}/"

        try:
            self.logger.info(command_upload_to_gcs)
            subprocess.run(command_upload_to_gcs, shell=True, check=True)
        except FileNotFoundError:
            self.logger.error("Error while uploading the file to GCS", exc_info=True)
            sys.exit(1)

        os.remove(f"{file_directory}{file_name}")

    def split_upload_report_gcs(
        self, report_date: str, zuora_report_list_to_download: Dict
    ) -> None:
        """This function is responsible for split the downloaded output file based on the static_rows_column_header in yaml file and upload it to GCS bucket."""

        file_directory = (
            self.zuora_report_output_directory + report_date.replace("-", "_") + "/"
        )
        requested_report_list = self.get_report_list_key(zuora_report_list_to_download)
        # list to store files
        list_of_files = self.get_all_downloaded_file_list(file_directory)
        for report in requested_report_list:
            report_list_dict = self.get_report_list_dict(
                zuora_report_list_to_download, report
            )
            report_file_name = report_list_dict.get("output_file_name")
            report_static_column_list = report_list_dict.get(
                "static_rows_column_header"
            )
            for file in list_of_files:
                if file.startswith(report_file_name):
                    self.split_file(file_directory, file, report_static_column_list)

        list_of_file_to_upload = self.get_file_list_to_upload(file_directory)
        for file_name in list_of_file_to_upload:
            self.upload_delete(file_directory, file_name, report_date.replace("-", "_"))
