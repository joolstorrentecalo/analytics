import sys
import os
import fire
import requests
import pandas as pd
import xmltodict

from logging import info, error, basicConfig, getLogger
from io import StringIO
from typing import Any, Dict, Union, List, Optional
from gitlabdata.orchestration_utils import make_request

from adaptive_helpers import (
    upload_exported_data,
    read_processed_versions_table,
    upload_processed_version,
    edit_dataframe,
)


config_dict = os.environ.copy()


class Adaptive:
    def __init__(self):
        pass

    def _base_xml(self, method: str, additional_body: str) -> str:
        """Every endpoint needs to be called with this base xml argument"""
        username = config_dict["ADAPTIVE_USERNAME"]
        password = config_dict["ADAPTIVE_PASSWORD"]
        caller_name = "Data"

        xml_string = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            + f'<call method="{method}" callerName="{caller_name}">'
            + f'<credentials login="{username}" password="{password}"/>'
            + additional_body
            + "</call>"
        )

        return xml_string

    def handle_response(self, response: requests.models.Response) -> Union[dict, str]:
        """API returns a xml string, check the xml string for success"""
        parsed_dict = xmltodict.parse(response.text)
        success = parsed_dict["response"]["@success"]
        info(f"\nIs successful response: {success}")

        if success == "false":
            error_message = parsed_dict["response"]["messages"]["message"]["#text"]
            info(f"\nAborting... error_message: {error_message}")
            sys.exit()

        else:
            export_output = parsed_dict["response"]["output"]
        return export_output

    def _export(self, method: str, additional_body: str) -> Union[dict, str]:
        """Generic function to export data from any Adaptive endpoint"""
        url = "https://api.adaptiveinsights.com/api/v29"
        headers = {"Content-Type": "application/xml"}
        xml_string = self._base_xml(method, additional_body)

        response = make_request("POST", url, headers=headers, data=xml_string)

        export_output = self.handle_response(response)
        return export_output

    def export_versions(self) -> Dict[Any, Any]:
        """Call exportVersions endpoint to get list of versions"""
        method = "exportVersions"
        additional_body = ""
        info("Calling export_version endpoint...")
        versions_output = self._export(method, additional_body)
        if isinstance(versions_output, dict):
            versions = versions_output["versions"]
        return versions

    def export_data(self, version_name: str) -> str:
        """Call exportData endpoint to the data associated with a version"""
        method = "exportData"
        account_codes = [
            "RPO",
            "cRPO",
            "Revenue",
            "Bookings_Model_New.Plan_Contraction_and_Churn",
            "new_logos",
            "Bookings_Model_New.BM_Exit_ARR",
            "Bookings_Model_New.BM_Net_ARR_Bookings",
            "Bookings_Model_New.5K_Customer_Number",
            "Bookings_Model_New.100K_Customer_Number",
            "Headcount",
            "Current_Billings",
            "Total_Calc_Billings",
        ]

        account_codes_xml = "".join(
            [
                '<account code="{}" isAssumption="false" includeDescendants="false"/>'.format(
                    code
                )
                for code in account_codes
            ]
        )

        additional_body = (
            f'<version name="{version_name}" isDefault="false"/>'
            + '<format useInternalCodes="true" includeUnmappedItems="false"/>'
            + "<filters>"
            + "<accounts>"
            + account_codes_xml
            + "</accounts>"
            + "<levels>"
            + '<level name="Master Corporate" isRollup="true" includeDescendants="false"/>'
            + "</levels>"
            # + '<timeSpan start="02/2023" end="01/2024"/>'
            + "</filters>"
            + '<rules includeRollupAccounts="true" includeRollupLevels="true"> </rules>'
        )

        info("Calling export_data endpoint...")
        data = self._export(method, additional_body)
        return str(data)  # coercing str -> str to remove mypy error

    def _iterate_versions(self, versions: List[Any], version_reports: List[Any]):
        """
        Versions is a json structure of nested list of dictionaries
        that represents a folder structure of versions.

        This function loops through this nested data structure, and returns
        the name of each version.
        """
        if type(versions) == dict:
            versions = [versions]

        for version_d in versions:
            if version_d["@type"] == "VERSION_FOLDER" and version_d.get("version"):
                self._iterate_versions(
                    version_d["version"],
                    version_reports,
                )
            else:
                version_reports.append(version_d["@name"])

    def _filter_for_subfolder(
        self, versions: Union[List[Any], Dict[Any, Any]], folder_criteria: str
    ) -> Optional[List[Any]]:
        """
        @versions: a nested list of dictionaries, representing a file struct
        @folder_criteria: which sub-folder to filter for

        Filters for just the sub-folder from versions
        returning a piece of the original nested json
        """
        if folder_criteria is None:
            return versions

        if type(versions) == dict:
            versions = [versions]

        for version_d in versions:
            if version_d["@type"] == "VERSION_FOLDER" and version_d.get("version"):
                foldername = version_d["@name"]
                inner_versions = version_d["version"]
                if foldername == folder_criteria:
                    return inner_versions
                res = self._filter_for_subfolder(inner_versions, folder_criteria)
                if res:  # only return if match was found
                    return res
        return None

    def get_valid_versions(
        self, versions: Dict[Any, Any], folder_criteria: str
    ) -> List[Any]:
        """
        @versions: a nested list of dictionaries, representing a file struct
        @folder_criteria: which sub-folder to filter for

        First filters for the json that represents the sub-folder.
        From that filtered json, extract all the version names
        """

        root_versions = versions["version"]
        filtered_versions = self._filter_for_subfolder(root_versions, folder_criteria)
        if filtered_versions is None:  # no versions match folder_criteria
            return []

        version_reports: List[Any] = []
        self._iterate_versions(filtered_versions, version_reports)
        return version_reports

    def exported_data_to_df(self, exported_data: str) -> pd.DataFrame:
        """
        exported_data is a string that partially contains csv delimited data
        Take the csv delimited data and convert to pandas dataframe
        """
        # exported_data.lstrip("![CDATA[").rstrip("]]")
        exported_data_io = StringIO(exported_data)
        dataframe = pd.read_csv(exported_data_io)
        info(f"\nresponse dataframe.head(): {dataframe.head()}")
        return dataframe

    def is_version_already_processed(self, version: str) -> bool:
        """
        Check if the version is already processed by checking the processed table
        """
        dataframe = read_processed_versions_table()
        if version in dataframe["version"].values:
            return True
        return False

    def process_version(self, version: str):
        """
        For a version, do the following:
            1. export report from API
            2. Convert to dataframe
            3. Edit the dataframe
            4. Upload the dataframe
            5. Record processed version (so not to process it again)
        """
        if self.is_version_already_processed(version):
            info(f"\nAlready processed, skipping version: {version}")
            return

        info(f"\nprocessing version: {version}")
        exported_data = self.export_data(version)
        try:
            dataframe = self.exported_data_to_df(exported_data)
            dataframe = edit_dataframe(dataframe, version)
        # ValueError thrown if the columns are not formatted mo/year
        except ValueError as e:
            error(f"\nError processing {version}:\n{e}")
            return

        upload_exported_data(dataframe)
        upload_processed_version(version)
        info(f"\nfinished processing: {version}")

    def process_versions(self, folder_criteria: str):
        """
        folder_criteria is the top level folder to extract all versions from
        For each version, export the data
        Then upload the data to Snowflake
        Lastly, add verison to processed table so it's not processed again
        """
        versions = self.export_versions()
        valid_versions = self.get_valid_versions(versions, folder_criteria)
        info(
            f"\nversions to process (if not processed in previous runs):\n {valid_versions}"
        )
        for valid_version in valid_versions:
            self.process_version(valid_version)


def main(version_criteria=None, folder_criteria=None):
    """
     Args:
         version_criteria: the version to process
         folder_criteria: the folder to process (that contains all the versions)

    Main function to run the export.
    Either export one version, or export all unprocessed versions
    """
    adaptive = Adaptive()

    if version_criteria:
        adaptive.process_version(version_criteria)

    # export all versions in a folder (including subfolders)
    elif folder_criteria:
        adaptive.process_versions(folder_criteria)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(main)
    info("Complete.")
