import logging
import json
from json.decoder import JSONDecodeError
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
from typing import Any, Dict
import pandas as pd


logger = logging.getLogger(__name__)


class FetchPublishedDate:

    def __init__(
            self,
            canvas_url: str,
            canvas_token: str,
            num_workers: int,
            course_data_from_api: pd.DataFrame,
            course_data_from_db: pd.DataFrame,
            retry_attempts: int
    ):
        self.canvas_url: str = canvas_url
        self.canvas_token: str = canvas_token
        self.course_data_from_api: pd.DataFrame = course_data_from_api
        self.course_data_from_db: pd.DataFrame = course_data_from_db
        self.retry_attempts: int = retry_attempts
        self.num_workers: int = num_workers
        self.published_course_date: Dict[int, str] = {}
        # { course_id: {'url': 'https://instructure.com','count': 0} }
        self.published_date_retry_bucket: Dict[int, Dict[str, Any]] = {}

    def get_next_page_url(self, response) -> None:
        """
        get the next page url from the Http response headers
        :param response:
        :type response: requests
        :return: next_page_url
        :rtype: str
        """
        logging.debug(self.get_next_page_url.__name__ + '() called')
        result = response.result()
        links = result.links
        if not links:
            logging.debug('The api call do not have Link headers')
            return None

        course_id = int(result.url.split('?')[0].split('/')[-1])
        if 'next' in links:
            url_ = links['next']['url']
            logger.debug('Fetching the next page URL')
            # This update or add to the retry bucket
            self.published_date_retry_bucket[course_id] = {
                'url': url_,
                'count': 0,
            }
            logger.info(f"Retry list size {len(self.published_date_retry_bucket)} for published_at date")
        else:
            if course_id in self.published_date_retry_bucket:
                logger.info(f"Removing the course {course_id} from the list as course don't have a date {links}")
                self.published_date_retry_bucket.pop(course_id)
            else:
                # This is the case when canvas sends no Date for a course
                logger.info(f"Course {course_id} don't have published date")

    def published_date_resp_parsing(self, response) -> None:
        if response is None:
            logger.info(f"Published course date response is None ")
            return

        logger.info(f"published courses date collected so far : {len(self.published_course_date)}")
        response_result = response.result()
        url = response_result.url
        course_id = int(url.split('?')[0].split('/')[-1])

        logger.info(f"Parsing the response for Course: {course_id}")
        logger.debug(f"Pagination info {course_id} {response_result.links}")
        logger.info(f"Time taken to get the response for {course_id} : {response_result.elapsed}")
        status = response_result.status_code
        published_date_found = False
        if status != 200:
            logger.warning(f"Request was unsuccessful for {course_id}")
            logger.warning(f"Response status: {status}; time taken: {response_result.elapsed}; response text: {response_result.text}")
            self.retry_logic_with_error(course_id, url)
            return

        try:
            audit_events = json.loads(response_result.text)
        except JSONDecodeError as e:
            logger.error(f"Error in parsing the response {e.msg}")
            self.retry_logic_with_error(course_id, url)
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
                if course_id in self.published_date_retry_bucket:
                    logger.info(f"Going to remove {course_id} from retry list {len(self.published_date_retry_bucket)}")
                    self.published_date_retry_bucket.pop(course_id)
                    logger.info(f"Removed {course_id} removed from retry list {len(self.published_date_retry_bucket)}")
                break
        if not published_date_found:
            self.get_next_page_url(response)

        return

    def retry_logic_with_error(self, course_id, url) -> None:
        if course_id in self.published_date_retry_bucket:
            retry_count = self.published_date_retry_bucket[course_id]['count']
            if retry_count >= self.retry_attempts:
                # Don't want to retry more
                logger.info(f"Reached max retry attempts for {course_id} with count {retry_count}, Removing... ")
                self.published_date_retry_bucket.pop(course_id)
            else:
                # We will give one more chance to retry
                logger.info(f"Another retry attempt will be made for {course_id}")
                self.published_date_retry_bucket[course_id]['count'] += 1
        else:
            logger.info(f"Adding course {course_id} to retry bucket")
            self.published_date_retry_bucket[course_id] = {
                'url': url,
                'count': 1,
            }
        logger.info(f"Retry list size {len(self.published_date_retry_bucket)} for published_at date")

    def filter_courses_to_fetch_published_date(self) -> pd.DataFrame:
        logger.info(f"Size of courses data from API routine: {self.course_data_from_api.shape}")
        available_courses_from_api = self.course_data_from_api[
            (self.course_data_from_api['workflow_state'] == 'available')].shape
        logger.info(f"Size of published courses from API: {available_courses_from_api}")
        logger.info(f"Size of published courses from DB with: {self.course_data_from_db.shape}")
        published_date_in_db = self.course_data_from_db[(self.course_data_from_db['published_at'].notnull())].shape
        logger.info(f"Size of published courses from DB with published date: {published_date_in_db}")
        course_with_pub_date_added_from_df = pd.merge(self.course_data_from_api, self.course_data_from_db,
                                                      on='canvas_id', how='left')
        logger.info(f"Size of course data after merging with DB data: {course_with_pub_date_added_from_df.shape}")
        return course_with_pub_date_added_from_df

    def get_published_course_date(self, course_ids, retry_list: Dict[int, str] = None) -> None:
        logger.info("Starting of get_published_course_date from API call")

        with FuturesSession(max_workers=self.num_workers) as future_session:
            headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + self.canvas_token}
            if retry_list is not None:
                logger.info("Going through error and pagination list")
                responses = [future_session.get(retry_list[course]['url'], headers=headers)
                             for course in retry_list]
            else:
                logger.info("Initial Round of Fetching course published date")
                responses = [
                    future_session.get(f'{self.canvas_url}/api/v1/audit/course/courses/{course_id}?per_page=100',
                                       headers=headers)
                    for course_id in course_ids
                ]

            for response in as_completed(responses):
                self.published_date_resp_parsing(response)

        if len(self.published_date_retry_bucket) != 0:
            logger.info(f"""Retrying now with list size {len(self.published_date_retry_bucket)} 
                        {self.published_date_retry_bucket}""")
            self.get_published_course_date(course_ids, self.published_date_retry_bucket)

    def get_published_date(self) -> pd.DataFrame:
        logger.info("Getting into fetching published date routine")
        courses_with_pub_date_col_df = self.filter_courses_to_fetch_published_date()
        is_published_date_all_empty = courses_with_pub_date_col_df['published_at'].isnull().all()
        published_date_in_db = courses_with_pub_date_col_df[
            (courses_with_pub_date_col_df['published_at'].notnull())].shape
        course_avail_with_no_pub_date_df = courses_with_pub_date_col_df.loc[
            (courses_with_pub_date_col_df['workflow_state'] == 'available') &
            (courses_with_pub_date_col_df['published_at'].isnull())].copy(deep=True)
        course_avail_with_no_pub_date_list = course_avail_with_no_pub_date_df['canvas_id'].to_list()
        logger.info(f"Published dates going to be fetched are: {len(course_avail_with_no_pub_date_list)}")
        if len(course_avail_with_no_pub_date_list) == 0:
            courses_with_pub_date_col_df = courses_with_pub_date_col_df[['canvas_id', 'published_at']]
            logger.info("No more published date to fetch than what is stored in DB")
            logger.info(f"Database should have {published_date_in_db[0]} published dates")
            return courses_with_pub_date_col_df

        self.get_published_course_date(course_avail_with_no_pub_date_list)

        if len(self.published_course_date) > 0:
            course_published_date_df = pd.DataFrame(self.published_course_date.items(),
                                                    columns=['canvas_id', 'published_at'])
            logger.info("newly fetched published data")
            course_published_date_df['published_at'] = pd.to_datetime(course_published_date_df['published_at'],
                                                                      format='%Y-%m-%dT%H:%M:%SZ',
                                                                      errors='coerce')
            courses_with_pub_date_col_df = pd.merge(courses_with_pub_date_col_df, course_published_date_df,
                                                    on='canvas_id', how='left')
            if is_published_date_all_empty:
                courses_with_pub_date_col_df['published_at'] = courses_with_pub_date_col_df['published_at_y']
            else:
                courses_with_pub_date_col_df['published_at'] = courses_with_pub_date_col_df['published_at_x'].fillna(
                    courses_with_pub_date_col_df['published_at_y'])

            courses_with_pub_date_col_df = courses_with_pub_date_col_df.drop(['published_at_x', 'published_at_y'],
                                                                             axis=1)
        logger.info(f"Size of newly published dates fetched from API: {len(self.published_course_date)}")
        logger.info(
            f"There are now {published_date_in_db[0] + len(self.published_course_date)} published dates")
        courses_with_pub_date_col_df = courses_with_pub_date_col_df[['canvas_id', 'published_at']]
        return courses_with_pub_date_col_df
