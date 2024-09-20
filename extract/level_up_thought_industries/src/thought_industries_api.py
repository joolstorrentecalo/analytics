"""
ThoughtIndustries is the name of the vendor that provides
GitLab with Learning Management System internally known as Level Up.


The code will refer to ThoughtIndustries when referring to the API,
and Level Up when referring to the schemas/tables to save.

There is one parent class- ThoughtIndustries- and for each API endpoint,
a corresponding child class.

The parent class contains the bulk of the logic as the endpoints are very similiar.
"""

import os
from abc import ABC, abstractmethod
from datetime import datetime
from logging import info
from typing import Dict, List, Tuple

from gitlabdata.orchestration_utils import make_request
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import text, quoted_name
from thought_industries_api_helpers import (
    epoch_ts_ms_to_datetime_str,
    get_metadata_engine,
    is_invalid_ms_timestamp,
    iso8601_to_epoch_ts_ms,
    upload_dict_to_snowflake,
)

config_dict = os.environ.copy()


class ThoughtIndustries(ABC):
    """Base abstract class that contains the main endpoint logic"""

    BASE_URL = "https://university.gitlab.com/"
    HEADERS = {
        "Authorization": f'Bearer {config_dict["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"]}'
    }
    RECORD_THRESHOLD = 4999  # will upload when record_count exceeds this threshold
    MAX_RETRY_COUNT = 7
    SCHEMA_NAME = "level_up"
    STAGE_NAME = "level_up_load_stage"

    @abstractmethod
    def get_endpoint_url(self):
        """Each child class must implement"""

    @abstractmethod
    def get_name(self):
        """Each child class must implement"""

    def __init__(self):
        """Instantiate instance vars"""
        self.name = self.get_name()
        self.endpoint_url = self.get_endpoint_url()

    def base_upload_to_snowflake(self, upload_dict: dict, batch: int):
        """Base function to upload dict to Snowflake as Variant"""
        table_name = self.name
        json_dump_filename = f"level_up_{self.name}.json"

        info(
            f"Uploading batch {batch} with {len(upload_dict['data'])} records to Snowflake"
        )
        upload_dict_to_snowflake(
            upload_dict=upload_dict,
            schema_name=self.SCHEMA_NAME,
            stage_name=self.STAGE_NAME,
            table_name=table_name,
            json_dump_filename=json_dump_filename,
        )


# ------------- Cursor-based endpoints -------------
class CursorEndpoint(ThoughtIndustries):
    """
    Class used to provide functions to call cursor-based endpoints

    The cursor position is saved within a metadata database
    so that the position can be retrieved in future runs.
    """

    ENDPOINT_PREFIX = f"{ThoughtIndustries.BASE_URL}incoming/v2/"
    METADATA_SCHEMA = os.environ.get("LEVEL_UP_METADATA_SCHEMA")

    def __init__(self):
        super().__init__()
        self.results_key = self.name

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}{self.name}"

    def get_cursor_url(self, cursor: str) -> str:
        """
        In order to query for a page, the cursor must be passed into url
        This function returns the properply formatted cursor url

        If there is no cursor (first page of api call), just return the endpoint_url
        """
        if cursor:
            formatted_cursor = f"?cursor={cursor}"
            return f"{self.endpoint_url}{formatted_cursor}"
        return self.endpoint_url

    def read_cursor_state(self, metadata_engine: Engine) -> str:
        """
        query the database to see if there's an existing cursor page
            - If there's an existing cursor return it
            - Else return blank string
        """
        safe_schema = quoted_name(self.METADATA_SCHEMA, quote=True)

        query = text(
            f"""
        SELECT cursor_id
        FROM {safe_schema}.cursor_state
        WHERE endpoint = :endpoint
        ORDER BY uploaded_at DESC
        LIMIT 1;
        """
        )

        query_params = {"endpoint": self.name}
        with metadata_engine.connect() as connection:
            results = connection.execute(query, query_params).fetchall()
        cursor = results[0][0] if results else ""
        info(f"Cursor starting position: {cursor}")
        return cursor

    def write_cursor_state(self, cursor: str, metadata_engine: Engine):
        """
        After uploading to Snowflake, save the cursor into metadata db
        Note: not all processed cursors are saved, just the most recent
        after Snowflake upload.
        """
        safe_schema = quoted_name(self.METADATA_SCHEMA, quote=True)

        query = text(
            f"""
            INSERT INTO {safe_schema}.cursor_state (endpoint, cursor_id, uploaded_at)
            VALUES (:endpoint, :cursor_id, :uploaded_at);
            """
        )
        query_params = {
            "endpoint": self.name,
            "cursor_id": cursor,
            "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
        }

        with metadata_engine.connect() as connection:
            connection.execute(query, query_params)
        info(f"Wrote cursor {cursor} to cursor_state table")

    def fetch_from_endpoint(self, metadata_engine: Engine) -> Tuple[list, str, bool]:
        """Return results from cursor-based endpoints"""
        combined_results: List[Dict] = []
        has_more = True
        cursor = self.read_cursor_state(metadata_engine)

        while has_more and len(combined_results) <= self.RECORD_THRESHOLD:
            cursor_url = self.get_cursor_url(cursor)
            info(f"Making request to cursor_url: {cursor_url}")
            response = make_request(
                "GET",
                cursor_url,
                headers=self.HEADERS,
                timeout=60,
                max_retry_count=self.MAX_RETRY_COUNT,
            )

            results = response.json().get(self.results_key)
            page_info = response.json().get("pageInfo")

            # response has events
            if results:
                combined_results = combined_results + results

            has_more = page_info["hasMore"]
            if has_more:
                cursor = page_info["cursor"]

        return combined_results, cursor, has_more

    def upload_to_snowflake(self, results: list, batch: int):
        """Upload dictionary to Snowflake"""
        upload_dict = {
            "data": results,
            "data_interval_start": os.environ["data_interval_start"],
            "data_interval_end": os.environ["data_interval_end"],
        }
        self.base_upload_to_snowflake(upload_dict, batch)

    def fetch_and_upload_data(self):
        """Fetch data and upload it to Snowflake"""
        has_more, batch = True, 0
        metadata_engine = get_metadata_engine()

        while has_more:
            results, cursor, has_more = self.fetch_from_endpoint(metadata_engine)
            if results:
                batch += 1
                self.upload_to_snowflake(results, batch)
            # upon successful snowflake upload, write cursor state
            self.write_cursor_state(cursor, metadata_engine)

        if batch == 0:
            info("No results data returned, nothing to upload")
        info(f"has_more={has_more}, there should be no more results to extract")


class Users(CursorEndpoint):
    """Class for Users endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "users"


class Content(CursorEndpoint):
    """Class for Users endpoint"""

    def __init__(self):
        super().__init__()
        self.results_key = "contentItems"

    def get_name(self) -> str:
        """implement abstract class"""
        return "content"


class Meetings(CursorEndpoint):
    """Class for Users endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "meetings"


class Clients(CursorEndpoint):
    """Class for Users endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "clients"


class AssessmentAttempts(CursorEndpoint):
    """Class for Users endpoint"""

    def __init__(self):
        super().__init__()
        self.results_key = "assessmentAttempts"

    def get_name(self) -> str:
        """implement abstract class"""
        return "assessment_attempts"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}assessmentAttempts"


class Coupons(CursorEndpoint):
    """Class for Users endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "coupons"


# ------------- Data Interval-based endpoints -------------
class DateIntervalEndpoint(ThoughtIndustries):
    """
    Parent class for data interval endpoints, i.e endpoints
    where a start and end date are used to select what data is returned
    """

    ENDPOINT_PREFIX = f"{ThoughtIndustries.BASE_URL}incoming/v2/events/"

    def fetch_from_endpoint(
        self, original_epoch_start_ms: int, original_epoch_end_ms: int
    ) -> Tuple[List[Dict], int]:
        """
        Based on the start & end epoch dates, continue calling the API
        until no more data is returned.

        Note that:
        - the API returns data from latest -> earliest
        - only returns 100 records per request.

        The sliding window of start/end times looks like this:
        Start ————————— end  # first request
        Start ————— prev_min  # 2nd request
        Start —- prev_min # 3rd request
        ...
        """
        combined_results: List[Dict] = []
        results: List[Dict] = [{-1: "_"}]  # some placeholder val
        current_epoch_end_ms = original_epoch_end_ms  # init current epoch end
        # while the response returns results records
        # also check len(combined_results) because of Snowflake VARIANT value max size
        while len(results) > 0 and len(combined_results) <= self.RECORD_THRESHOLD:
            params = {
                "startDate": original_epoch_start_ms,
                "endDate": current_epoch_end_ms,
            }
            info(f"\nMaking request to {self.endpoint_url} with params:\n{params}")
            response = make_request(
                "GET",
                self.endpoint_url,
                headers=self.HEADERS,
                params=params,
                timeout=60,
                max_retry_count=self.MAX_RETRY_COUNT,
            )

            results = response.json().get("events")

            # response has results
            if results:
                combined_results = combined_results + results

                prev_epoch_end_ms = current_epoch_end_ms

                # current_epoch_end will be the previous earliest timestamp
                current_epoch_end_ms = (
                    iso8601_to_epoch_ts_ms(results[-1]["timestamp"]) - 1
                )  # subtract by 1 sec from ts so that record isn't included again
                # the endDate should be getting smaller each call
                if current_epoch_end_ms >= prev_epoch_end_ms:
                    # raise error if endDate stayed the same or increased
                    raise ValueError(
                        "endDate parameter has not changed since last call."
                    )
            # no more results in response, should stop making requests
            else:
                info("\nThe last response had 0 results, stopping requests\n")

        return combined_results, current_epoch_end_ms

    def upload_to_snowflake(
        self, results: List[Dict], epoch_start_ms: int, epoch_end_ms: int, batch: int
    ):
        """Upload event dict to Snowflake"""

        api_start_datetime = epoch_ts_ms_to_datetime_str(epoch_start_ms)
        api_end_datetime = epoch_ts_ms_to_datetime_str(epoch_end_ms)
        upload_dict = {
            "data": results,
            "api_start_datetime": api_start_datetime,
            "api_end_datetime": api_end_datetime,
        }

        self.base_upload_to_snowflake(upload_dict, batch)
        info(
            f"Completed writing to Snowflake for api_start_datetime {api_start_datetime} "
            f"& api_end_datetime {api_end_datetime}"
        )

    def fetch_and_upload_data(
        self, original_epoch_start_ms: int, original_epoch_end_ms: int
    ) -> List:
        """
        main function, fetch data from API, and upload to snowflake.
        This was updated to upload in batches based on `RECORD_THRESHOLD`
        This is necessary because Snowflake has a max size limit per VARIANT value

        In the future, if `all_results` object becomes too big, it can be easily removed
        However, it's currently useful for debugging
        """
        if is_invalid_ms_timestamp(original_epoch_start_ms, original_epoch_end_ms):
            raise ValueError(
                "Invalid epoch timestamp(s). Make sure epoch timestamp is in MILLISECONDS. "
                "Aborting now..."
            )

        results: List = [{}]
        all_results: List = []
        batch = 0

        current_epoch_end_ms = original_epoch_end_ms

        while results:
            results, current_epoch_end_ms = self.fetch_from_endpoint(
                original_epoch_start_ms, current_epoch_end_ms
            )

            if results:
                batch += 1
                self.upload_to_snowflake(
                    results, original_epoch_start_ms, original_epoch_end_ms, batch
                )
                all_results = all_results + results

        if batch == 0:
            info("No results data returned, nothing to upload")
        return all_results


class CourseCompletions(DateIntervalEndpoint):
    """Class for CourseCompletions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_completions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}courseCompletion"


class Logins(DateIntervalEndpoint):
    """Class for Logins endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "logins"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}login"


class Visits(DateIntervalEndpoint):
    """Class for Visits endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "visits"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}visit"


class CourseViews(DateIntervalEndpoint):
    """Class for CourseViews endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_views"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}courseView"


class CourseActions(DateIntervalEndpoint):
    """Class for CourseActions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_actions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}courseAction"


class CoursePurchases(DateIntervalEndpoint):
    """Class for CoursePurchases endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_purchases"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}coursePurchase"


class LearningPathActions(DateIntervalEndpoint):
    """Class for LearningPathActions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "learning_path_actions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}learningPathAction"


class EmailCaptures(DateIntervalEndpoint):
    """Class for EmailCaptures endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "email_captures"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}emailCapture"


class CodeRedemptions(DateIntervalEndpoint):
    """Class for EmailCaptures endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "code_redemptions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return f"{self.ENDPOINT_PREFIX}redemptionCodeRedeem"


if __name__ == "__main__":
    # used for testing DateIntervalEndpoint
    EPOCH_START_MS = 1722384000001
    EPOCH_END_MS = 1722470400000
    course_actions = CourseActions()
    result_events = course_actions.fetch_and_upload_data(EPOCH_START_MS, EPOCH_END_MS)
    info(f"\nresult_events: {result_events[:2]}")

    # used for testing CursorEndpoint
    users = Users()
    users.fetch_and_upload_data()
