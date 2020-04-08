import logging
import time
import json
from json.decoder import JSONDecodeError
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed


logger = logging.getLogger(__name__)


class FetchPublishedDate:

    def __init__(self, canvas_url, canvas_token, num_workers, canvas_ids):
        self.canvas_url = canvas_url
        self.canvas_token = canvas_token
        self.canvas_ids = canvas_ids
        self.num_workers = num_workers
        self.published_course_date = {}
        self.published_course_next_page_list = []

    def get_next_page_url(self, response):
        """
        get the next page url from the Http response headers
        :param response:
        :type response: requests
        :return: next_page_url
        :rtype: str
        """
        logging.debug(self.get_next_page_url.__name__ + '() called')
        results = response.result().links
        if not results:
            logging.debug('The api call do not have Link headers')
            return None

        if 'next' in results:
            url_ = results['next']['url']
            self.published_course_next_page_list.append(url_)
            logger.info(f"Pagination size {len(self.published_course_next_page_list)} for published_at date")

    def published_date_resp_parsing(self, response):

        logger.info("published_date_resp_parsing Call")

        if response is None:
            logger.info(f"Published course date response is None ")
            return

        start_time = time.time()

        logger.info(f"published courses date collected so far : {len(self.published_course_date)}")
        status = response.result().status_code
        published_date_found = False
        if status != 200:
            logger.info(f"Response not successful with status code {status} due to {response.result().text}")
            return

        try:
            audit_events = json.loads(response.result().text)
        except JSONDecodeError as e:
            logger.error(f"Error in parsing the response {e.msg}")
            return

        if not audit_events:
            logger.info(f"Response for fetching published date is empty {audit_events}")
            return

        events = audit_events['events']

        # audit logs sends event data in descending order
        # https://canvas.instructure.com/doc/api/course_audit_log.html
        for event in events:
            if event['event_type'] == 'published':
                course_id = event['links']['course']
                published_date_found = True
                self.published_course_date.update({course_id: event['created_at']})
                logger.info(f"Published Date {event['created_at']} for course {course_id}")
                break
        if not published_date_found:
            self.get_next_page_url(response)

        seconds = time.time() - start_time
        str_time = time.strftime("%H:%M:%S", time.gmtime(seconds))
        logger.debug(f"Parsing the published date took {str_time} ")
        return

    def get_published_course_date(self, course_ids, next_page_links=None):
        logger.info("Starting of get_published_course_date call")
        with FuturesSession(max_workers=self.num_workers) as session:
            headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + self.canvas_token}
            if next_page_links is not None:
                logger.info("Going through Next page URL set")
                responses = []
                for next_page_link in next_page_links:
                    response = session.get(next_page_link, headers=headers)
                    self.published_course_next_page_list.remove(next_page_link)
                    responses.append(response)
            else:
                logger.info("Initial Round of Fetching course published date")
                responses = [
                    session.get(f'{self.canvas_url}/api/v1/audit/course/courses/{course_id}?per_page=100',
                                headers=headers)
                    for course_id in course_ids
                ]

            for response in as_completed(responses):
                self.published_date_resp_parsing(response)

        if len(self.published_course_next_page_list) != 0:
            logger.info(f"""Pagination size {len(self.published_course_next_page_list)} with published_at date items
                         {self.published_course_next_page_list}""")
            self.get_published_course_date(course_ids, self.published_course_next_page_list)

        return self.published_course_date
