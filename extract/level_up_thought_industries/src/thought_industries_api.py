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

from logging import info
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from gitlabdata.orchestration_utils import make_request

from thought_industries_api_helpers import (
    iso8601_to_epoch_ts_ms,
    epoch_ts_ms_to_datetime_str,
    upload_payload_to_snowflake,
    is_invalid_ms_timestamp,
)

config_dict = os.environ.copy()
RECORD_THRESHOLD = 9999  # will upload when record_count exceeds this threshold


class ThoughtIndustries(ABC):
    """Base abstract class that contains the main endpoint logic"""

    BASE_URL = "https://university.gitlab.com/"
    HEADERS = {
        "Authorization": f'Bearer {config_dict["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"]}'
    }

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
        combined_events: List[Dict] = []
        events: List[Dict] = [{-1: "_"}]  # some placeholder val
        current_epoch_end_ms = original_epoch_end_ms  # init current epoch end
        record_count = 0
        # while the response returns events records
        # also check record_count because of Snowflake VARIANT value max size
        while len(events) > 0 and record_count <= RECORD_THRESHOLD:
            params = {
                "startDate": original_epoch_start_ms,
                "endDate": current_epoch_end_ms,
            }
            full_url = f"{self.BASE_URL}{self.endpoint_url}"
            info(f"\nMaking request to {full_url} with params:\n{params}")
            response = make_request(
                "GET",
                full_url,
                headers=self.HEADERS,
                params=params,
                timeout=60,
                max_retry_count=7,
            )

            events = response.json().get("events")

            # response has events
            if events:
                record_count += len(events)

                events_to_log = [events[0]] + [events[-1]]
                info(f"\nfirst & last event from latest response: {events_to_log}")
                combined_events = combined_events + events

                prev_epoch_end_ms = current_epoch_end_ms

                # current_epoch_end will be the previous earliest timestamp
                current_epoch_end_ms = (
                    iso8601_to_epoch_ts_ms(events[-1]["timestamp"]) - 1
                )  # subtract by 1 sec from ts so that record isn't included again
                # the endDate should be getting smaller each call
                if current_epoch_end_ms >= prev_epoch_end_ms:
                    # raise error if endDate stayed the same or increased
                    raise ValueError(
                        "endDate parameter has not changed since last call."
                    )
            # no more events in response, should stop making requests
            else:
                info("\nThe last response had 0 events, stopping requests\n")

        return combined_events, current_epoch_end_ms

    def upload_events_to_snowflake(
        self, events: List[Dict], epoch_start_ms: int, epoch_end_ms: int
    ):
        """Upload event payload to Snowflake"""

        api_start_datetime = epoch_ts_ms_to_datetime_str(epoch_start_ms)
        api_end_datetime = epoch_ts_ms_to_datetime_str(epoch_end_ms)
        upload_dict = {
            "data": events,
            "api_start_datetime": api_start_datetime,
            "api_end_datetime": api_end_datetime,
        }

        schema_name = "level_up"
        stage_name = "level_up_load_stage"
        table_name = self.name
        json_dump_filename = f"level_up_{self.name}.json"
        upload_payload_to_snowflake(
            upload_dict, schema_name, stage_name, table_name, json_dump_filename
        )
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

        In the future, if `all_events` object becomes too big, it can be easily removed
        However, it's currently useful for debugging
        """
        if is_invalid_ms_timestamp(original_epoch_start_ms, original_epoch_end_ms):
            raise ValueError(
                "Invalid epoch timestamp(s). Make sure epoch timestamp is in MILLISECONDS. "
                "Aborting now..."
            )

        events_batched: List = [{}]
        all_events: List = []
        batch = 0

        current_epoch_end_ms = original_epoch_end_ms

        while events_batched:
            events_batched, current_epoch_end_ms = self.fetch_from_endpoint(
                original_epoch_start_ms, current_epoch_end_ms
            )

            if events_batched:
                batch += 1
                info(f"Uploading batch {batch} with {len(events_batched)} records...")
                self.upload_events_to_snowflake(
                    events_batched, original_epoch_start_ms, original_epoch_end_ms
                )
                all_events = all_events + events_batched

        if batch == 0:
            info("No events data returned, nothing to upload")
        return all_events


class CourseCompletions(ThoughtIndustries):
    """Class for CourseCompletions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_completions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/courseCompletion"


class Logins(ThoughtIndustries):
    """Class for Logins endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "logins"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/login"


class Visits(ThoughtIndustries):
    """Class for Visits endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "visits"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/visit"


class CourseViews(ThoughtIndustries):
    """Class for CourseViews endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_views"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/courseView"


class CourseActions(ThoughtIndustries):
    """Class for CourseActions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_actions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/courseAction"


class CoursePurchases(ThoughtIndustries):
    """Class for CoursePurchases endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "course_purchases"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/coursePurchase"


class LearningPathActions(ThoughtIndustries):
    """Class for LearningPathActions endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "learning_path_actions"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/learningPathAction"


class EmailCaptures(ThoughtIndustries):
    """Class for EmailCaptures endpoint"""

    def get_name(self) -> str:
        """implement abstract class"""
        return "email_captures"

    def get_endpoint_url(self) -> str:
        """implement abstract class"""
        return "incoming/v2/events/emailCapture"


if __name__ == "__main__":
    EPOCH_START_MS = 1722384000001
    EPOCH_END_MS = 1722470400000
    cls_to_run = CourseActions()
    result_events = cls_to_run.fetch_and_upload_data(EPOCH_START_MS, EPOCH_END_MS)
    info(f"\nresult_events: {result_events[:2]}")
