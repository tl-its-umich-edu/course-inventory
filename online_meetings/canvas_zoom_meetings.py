# Script to get all sites where Zoom is visible and retrieve the meetings to generate a report

import json
import yaml
import re
import os
import sys
import requests
import logging
import http.client
from datetime import datetime

from canvasapi import Canvas
from bs4 import BeautifulSoup as bs

import pandas as pd

# read configurations
try:
    with open(os.path.join(os.path.dirname(__file__), '../config/secrets/env.json')) as env_file:
        ENV = yaml.safe_load(env_file.read())
except FileNotFoundError:
    sys.exit(
        'Configuration file could not be found; please add env.json to the config directory.')

LOG_LEVEL = ENV.get('LOG_LEVEL', 'DEBUG')
logging.basicConfig(level=LOG_LEVEL)

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.
if LOG_LEVEL == logging.DEBUG:
    http.client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(LOG_LEVEL)
requests_log.propagate = True
# Global variable to access Canvas
CANVAS = Canvas(ENV.get("CANVAS_URL"), ENV.get("CANVAS_TOKEN"))


def zoom_course_report(canvas_account=1, enrollment_term_id=1, published=True):
    zoom_courses = []
    zoom_courses_meetings = []

    account = CANVAS.get_account(canvas_account)
    # Canvas has a limit of 100 per page on this API
    per_page = 100
    # Get all published courses from the defined enrollment term
    courses = account.get_courses(enrollment_term_id=enrollment_term_id, published=published, per_page=per_page)
    # For testing
    # course = CANVAS.get_course(331376)
    # courses = [course, ]
    course_count = 0
    for course in courses:
        course_count += 1
        # TODO: In the future get the total count from the Paginated object
        # Needs API support https://github.com/ucfopen/canvasapi/issues/114
        logger.info(f"Fetching course #{course_count} for {course}")
        # Get tabs and look for zoom
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

                fields = form.findAll('input')
                formdata = dict((field.get('name'), field.get('value')) for field in fields)

                # Get the URL to post back to
                posturl = form.get('action')

                # Start up the zoom session
                zoom_session = requests.Session()
                # Initiate the LTI launch to Zoom in a session
                r = zoom_session.post(url=posturl, data=formdata)

                # Get the XSRF Token
                pattern = re.search('"X-XSRF-TOKEN".* value:"(.*)"', r.text)

                zoom_courses.append({
                    'account_id': course.account_id, 'course_id': course.id, 'course_name': course.name
                })

                if pattern:
                    zoom_session.headers.update({
                        'X-XSRF-TOKEN': pattern.group(1)
                    })

                    # Page iterator
                    page_num = 1
                    while True:
                        logger.info(f"Paging though course on {page_num}")
                        # Get tab 1 (Previous Meetings)
                        data = {'page': page_num,
                                'total': 0,
                                'storage_timezone': 'America/Montreal',
                                'client_timezone': 'America/Detroit'}
                        r = zoom_session.get("https://applications.zoom.us/api/v1/lti/rich/meeting/history/COURSE/all", params=data)
                        zoom_json = json.loads(r.text)
                        # If the length of the list is empty we've run out of pages
                        if len(zoom_json["result"]["list"]) == 0:
                            logger.info("Done paging through list")
                            break
                        for meeting in zoom_json["result"]["list"]:
                            zoom_courses_meetings.append({
                                'course_id': course.id,
                                'meeting_id': meeting['meetingId'],
                                'meeting_number': meeting['meetingNumber'],
                                'host_id': meeting['hostId'],
                                'topic': meeting['topic'],
                                'join_url': meeting['joinUrl'],
                                'start_time': meeting['startTime'],
                                'status': meeting['status'],
                                'timezone': meeting['timezone']
                            })
                        page_num += 1

                else:
                    logger.warn("PATTERN NOT FOUND in course, no details logged")
                    logger.debug(r.text)

    return (zoom_courses, zoom_courses_meetings)


start_time = datetime.now()
logger.info(f"Script started at {start_time}")

(zoom_courses, zoom_courses_meetings) = zoom_course_report(ENV.get("CANVAS_ACCOUNT_ID", 1), ENV.get("CANVAS_TERM_ID", 1), True)

zoom_courses_df = pd.DataFrame(zoom_courses)
zoom_courses_df.index.name = "id"
zoom_courses_meetings_df = pd.DataFrame(zoom_courses_meetings)
zoom_courses_meetings_df.index.name = "id"

zoom_courses_df.to_csv("zoom_courses.csv")
zoom_courses_meetings_df.to_csv("zoom_courses_meetings.csv")

end_time = datetime.now()
logger.info(f"Script finished at {start_time}")
logging.info('Duration: {}'.format(end_time - start_time))
