# Script to get all sites External Tool (LTI) Placements in Canvas and generate a report

import json
import logging
import math
import os
import re
from typing import Dict, List, Optional, Sequence, Union

import canvasapi
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

from db.db_creator import DBCreator
from environ import ENV, DATA_DIR
from vocab import DataSourceStatus, ValidDataSourceName

logger = logging.getLogger(__name__)


class CanvasLtiPlacementProcessor:
    # Stores a list of all LTI placements to be written out
    lti_placements: List[Dict] = []
    # Stores a list of all courses/meetings for zoom to be written out
    zoom_courses_meetings: List[Dict] = []

    # Indexes to keep track of how many courses and tabs we've processed
    course_count: int = 0
    placement_count: int = 0

    def __init__(self,
                 canvas_url: str,
                 canvas_token: str,
                 supported_lti_tools: Union[Dict[str, Dict[str, Union[str, int]]], None]):
        self.canvas = canvasapi.Canvas(canvas_url, canvas_token)
        self.zoom_placements = ZoomPlacements(self.canvas)
        if not supported_lti_tools:
            supported_lti_tools = {}
        self.supported_lti_tools = supported_lti_tools

    def generate_lti_course_report(self,
                                   canvas_account_id: int,
                                   enrollment_term_ids: Union[Sequence[int], None],
                                   add_course_ids: Union[List[int], None],
                                   published: bool = True):
    
        account = self.canvas.get_account(canvas_account_id)
        # Canvas has a limit of 100 per page on this API
        per_page = 100

        # Get all published courses from the defined enrollment terms
        courses: List[canvasapi.course.Course] = []
        if enrollment_term_ids is not None:
            for enrollment_term_id in enrollment_term_ids:
                logger.info(f'Fetching published course data for term {enrollment_term_id}')
                courses_list = list(
                    account.get_courses(
                        enrollment_term_id=enrollment_term_id,
                        published=published,
                        per_page=per_page
                    )
                )
                courses += courses_list

        for course in courses:
            if add_course_ids and course.id in add_course_ids:
                add_course_ids.remove(course.id)
            # TODO: In the future get the total count from the Paginated object
            # Needs API support https://github.com/ucfopen/canvasapi/issues/114
            self.get_lti_tabs(course)

        # If there are course_ids passed in, also process those
        if add_course_ids:
            for course_id in add_course_ids:
                self.get_lti_tabs(self.canvas.get_course(course_id))
        return None

    def get_lti_tabs(self, course: canvasapi.course.Course) -> None:
        # This is a new course we're looking through
        self.course_count += 1
        logger.info(f"Fetching course #{self.course_count} for {course}")
        # Get tabs and look for defined tool(s) that aren't hidden
        tabs = course.get_tabs()
        for tab in tabs:
            # The format in canvas of ids is like
            # context_external_tool_12345. But we need the numeric part
            tab_id = tab.id.split('_')[-1]
            supported_tool = self.supported_lti_tools.get(tab_id, None)
            # Hidden only included if true
            if (supported_tool and not hasattr(tab, "hidden")):
                self.placement_count += 1
                self.lti_placements.append({'id': self.placement_count,
                                            'course_id': course.id,
                                            'account_id': course.account_id,
                                            'course_name': course.name,
                                            'placement_type_id': supported_tool.get("id", -1)
                                            })

                # TODO: Find a better way of running this just for zoom
                if (tab.label.upper() == "ZOOM"):
                    self.zoom_courses_meetings.extend(
                        self.zoom_placements.get_zoom_details(tab, self.placement_count))
        return None

    def output_report(self) -> None:
        db_creator: DBCreator = DBCreator(ENV['INVENTORY_DB'])

        lti_placement_df = pd.DataFrame(self.lti_placements)
        lti_placement_df = lti_placement_df.set_index("id")

        lti_zoom_meeting_df = pd.DataFrame(self.zoom_courses_meetings)
        lti_zoom_meeting_df.index.name = "id"

        if ENV.get('CREATE_CSVS', False):
            logger.info(f'Writing {len(lti_placement_df)} lti_placement records to CSV')
            lti_placement_df.to_csv(os.path.join(DATA_DIR, "lti_placement.csv"))
            logger.info(f'Writing {len(lti_zoom_meeting_df)} lti_zoom_meeting records to CSV')
            lti_zoom_meeting_df.to_csv(os.path.join(DATA_DIR, "lti_zoom_meeting.csv"))

        # For now until this process is improved just remove all the previous records
        logger.info('Emptying Canvas LTI data tables in DB')
        db_creator.drop_records(
            ['lti_placement', 'lti_zoom_meeting']
        )

        logger.info(f'Inserting {len(lti_placement_df)} lti_placement records to DB')
        lti_placement_df.to_sql("lti_placement", db_creator.engine, if_exists="append", index=True)
        logger.info(f'Inserted data into lti_placement table in {db_creator.db_name}')

        logger.info(f'Inserting {len(lti_zoom_meeting_df)} lti_zoom_meeting records to DB')
        lti_zoom_meeting_df.to_sql("lti_zoom_meeting", db_creator.engine, if_exists="append", index=True)
        logger.info(f'Inserted data into lti_zoom_meeting table in {db_creator.db_name}')


class ZoomPlacements():

    def __init__(self, canvas: canvasapi.Canvas):
        self.zoom_session = requests.Session()
        self.canvas = canvas

    def get_zoom_json(self, **kwargs) -> Optional[Dict]:
        """Retrieves data directly from Zoom. You need to have zoom_session already setup
        
        :param kwargs: Supplied additional parameters to pass to the API. Should at least supply page and lti_scid.
        :type kwargs: Dict
        :return: json result from the Zoom call
        :rtype: Optional[Dict]
        """
        if not kwargs:
            # Set empty array if not set
            kwargs = {"page": 1, "lti_scid": ""}
        logger.info(f"Paging though course on page number {kwargs.get('page')}")
        # Get tab 1 (Previous Meetings)
        # Zoom needs this lti_scid now as a parameter, pull it out of the header
        kwargs.update({'total': 0,
                       'storage_timezone': 'America/Montreal',
                       'client_timezone': 'America/Detroit',
                       })

        # TODO: Specify which page we want, currently hardcoded to previous meetings
        zoom_previous_url = "https://applications.zoom.us/api/v1/lti/rich/meeting/history/COURSE/all"
        r = self.zoom_session.get(zoom_previous_url, params=kwargs)
        # Load in the json and look for results
        zoom_json = json.loads(r.text)
        if zoom_json and "result" in zoom_json:
            return zoom_json["result"]
        return None

    @staticmethod
    def extract_from_js(key: str, text: str) -> Optional[str]:
        """Takes a javascript text and attempts to extract a key/value (May not work with everything)
        
        :param key: key to extract
        :type key: str
        :param text: text to search in
        :type text: str
        :return: The matching string or None if not found
        :rtype: Optional[str]
        """
        pattern = re.search(f'{key}.*"(.*)"', text)
        if pattern:
            return pattern.group(1)
        return None

    def get_zoom_details(self, tab: canvasapi.tab.Tab, lti_placement_id: int) -> List[Dict]:
        zoom_courses_meetings: List[Dict] = []
        # Start up the zoom session
        # Initiate the LTI launch to Zoom in a session

        logger.info("Found a course with zoom as %s", tab.id)

        r = self.canvas._Canvas__requester.request("GET", _url=tab.url)
        external_url = r.json().get("url")
        r = requests.get(external_url)
        # Parse out the form from the response
        soup = bs(r.text, 'html.parser')
        # Get the form and parse out all of the inputs
        form = soup.find('form')
        if not form:
            logger.info("Could not find a form to launch this zoom page, skipping")
            return []

        fields = form.findAll('input')
        form_data = dict((field.get('name'), field.get('value')) for field in fields)
        # Get the URL to post back to
        post_url = form.get('action')

        r = self.zoom_session.post(url=post_url, data=form_data)

        # Get the scid
        scid = self.extract_from_js("scid", r.text)
        token = self.extract_from_js("X-XSRF-TOKEN", r.text)

        # Get the XSRF Token
        if token and scid:
            self.zoom_session.headers.update({
                'X-XSRF-TOKEN': token
            })

            zoom_json = self.get_zoom_json(page=1, lti_scid=scid)
            # The first call to zoom returns total and pageSize, get the total pages by dividing
            if zoom_json:
                total = zoom_json["total"]
                total_pages = math.ceil(total / zoom_json["pageSize"]) + 1
            for page in range(1, total_pages):
                # Just skip the first call we've already called it, but still need to process
                if page != 1:
                    zoom_json = self.get_zoom_json(page=page, lti_scid=scid)
                if zoom_json:
                    for meeting in zoom_json["list"]:
                        zoom_courses_meetings.append({
                            'lti_placement_id': lti_placement_id,
                            'meeting_id': meeting['meetingId'],
                            'host_id': meeting['hostId'],
                            'start_time': meeting['startTime'],
                            'status': meeting['status'],
                        })

        else:
            logger.error("Required token not found, no details logged. Check to see if this user can access Zoom.")
            logger.debug(r.text)
        return zoom_courses_meetings


def main() -> Sequence[DataSourceStatus]:
    '''
    This method is invoked when its module is executed as a standalone program.
    '''

    canvas_env = ENV.get('CANVAS', {})
    lti_processor = CanvasLtiPlacementProcessor(
        canvas_env.get("CANVAS_URL"),
        canvas_env.get("CANVAS_TOKEN"),
        canvas_env.get("SUPPORTED_LTI_TOOLS", {}))

    lti_processor.generate_lti_course_report(
        canvas_env.get("CANVAS_ACCOUNT_ID", 1),
        canvas_env.get("CANVAS_TERM_IDS", []),
        canvas_env.get("ADD_COURSE_IDS", []),
        True)
    lti_processor.output_report()

    return [DataSourceStatus(ValidDataSourceName.CANVAS_LTI)]


if '__main__' == __name__:
    main()
