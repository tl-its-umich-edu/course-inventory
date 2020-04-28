# Script to get all sites where Zoom is visible and retrieve the meetings to generate a report

import json
import logging
import math
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import canvasapi
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup as bs

# read configurations
try:
    with open(os.path.join(os.path.dirname(__file__), '../config/secrets/env.json')) as env_file:
        ENV = yaml.safe_load(env_file.read())
except FileNotFoundError:
    sys.exit(
        'Configuration file could not be found; please add env.json to the config directory.')

LOG_LEVEL = ENV.get('LOG_LEVEL', 'DEBUG')
logging.basicConfig(level=LOG_LEVEL)

# You must initialize logging, otherwise you'll not see debug output.
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(LOG_LEVEL)
requests_log.propagate = True
# Global variable to access Canvas
CANVAS = canvasapi.Canvas(ENV.get("CANVAS_URL"), ENV.get("CANVAS_TOKEN"))


class ZoomPlacements:
    zoom_courses: List[Dict] = []
    zoom_courses_meetings: List[Dict] = []

    def __init__(self):
        self.zoom_session = requests.Session()

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

    def get_zoom_details(self, url: str, data: Dict[str, str], course_id: int):
        # Start up the zoom session
        # Initiate the LTI launch to Zoom in a session
        r = self.zoom_session.post(url=url, data=data)

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
                        self.zoom_courses_meetings.append({
                            'course_id': course_id,
                            'meeting_id': meeting['meetingId'],
                            'meeting_number': meeting['meetingNumber'],
                            'host_id': meeting['hostId'],
                            'topic': meeting['topic'],
                            'join_url': meeting['joinUrl'],
                            'start_time': meeting['startTime'],
                            'status': meeting['status'],
                            'timezone': meeting['timezone']
                        })

        else:
            logger.warn("Required script extraction not found, no details logged")
            logger.debug(r.text)

    def get_zoom_course(self, course: canvasapi.course.Course) -> None:
        # Get tabs and look for defined tool(s) that aren't hidden
        tabs = course.get_tabs()
        for tab in tabs:
            # Hidden only included if true
            if (tab.label == "Zoom" and not hasattr(tab, "hidden")):
                logger.info("Found a course with zoom as %s", tab.id)

                r = CANVAS._Canvas__requester.request("GET", _url=tab.url)
                external_url = r.json().get("url")
                r = requests.get(external_url)
                # Parse out the form from the response
                soup = bs(r.text, 'html.parser')
                # Get the form and parse out all of the inputs
                form = soup.find('form')
                if not form:
                    logger.info("Could not find a form to launch this zoom page, skipping")
                    break

                self.zoom_courses.append({'account_id': course.account_id,
                                          'course_id': course.id,
                                          'course_name': course.name})

                fields = form.findAll('input')
                formdata = dict((field.get('name'), field.get('value')) for field in fields)
                # Get the URL to post back to
                posturl = form.get('action')
                self.get_zoom_details(posturl, formdata, course.id)
        return None

    def zoom_course_report(
        self,
        canvas_account: int = 1,
        enrollment_term_ids: Sequence[int] = [],
        published: bool = True,
        add_course_ids: list = None
    ) -> None:

        account = CANVAS.get_account(canvas_account)
        # Canvas has a limit of 100 per page on this API
        per_page = 100

        # Get all published courses from the defined enrollment terms
        courses = []
        if len(enrollment_term_ids) > 0:
            for enrollment_term_id in enrollment_term_ids:
                courses_list = list(
                    account.get_courses(
                        enrollment_term_id=enrollment_term_id,
                        published=published,
                        per_page=per_page
                    )
                )
                courses += courses_list

        course_count = 0
        for course in courses:
            course_count += 1
            if add_course_ids and course.id in add_course_ids:
                add_course_ids.remove(course.id)
            # TODO: In the future get the total count from the Paginated object
            # Needs API support https://github.com/ucfopen/canvasapi/issues/114
            logger.info(f"Fetching course #{course_count} for {course}")
            self.get_zoom_course(course)

        # If there are course_ids passed in, also process those
        if add_course_ids:
            for course_id in add_course_ids:
                self.get_zoom_course(CANVAS.get_course(course_id))
        return None


start_time = datetime.now()
logger.info(f"Script started at {start_time}")
zoom_placements = ZoomPlacements()
zoom_placements.zoom_course_report(ENV.get("CANVAS_ACCOUNT_ID", 1), ENV.get("CANVAS_TERM_IDS", []),
                                   True, ENV.get("ADD_COURSE_IDS", []))

zoom_courses_df = pd.DataFrame(zoom_placements.zoom_courses)
zoom_courses_df.index.name = "id"
zoom_courses_meetings_df = pd.DataFrame(zoom_placements.zoom_courses_meetings)
zoom_courses_meetings_df.index.name = "id"

zoom_courses_df.to_csv("zoom_courses.csv")
zoom_courses_meetings_df.to_csv("zoom_courses_meetings.csv")

end_time = datetime.now()
logger.info(f"Script finished at {start_time}")
logging.info('Duration: {}'.format(end_time - start_time))
