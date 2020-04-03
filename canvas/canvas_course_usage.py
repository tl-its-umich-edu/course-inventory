import logging
import time
import pandas as pd
from json.decoder import JSONDecodeError
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import json
logger = logging.getLogger(__name__)


class CanvasCourseUsage:
    def __init__(self, canvas_url, canvas_token, retry_attempts, course_ids):
        self.canvas_url = canvas_url,
        self.canvas_token = canvas_token,
        self.course_ids = course_ids
        self.retry_attempts = retry_attempts
        self.canvas_usage_courses = []
        self.course_retry_list = []
        self.retry_count = 0

    def parsing_canvas_course_usage_data(self, response):
        logger.info("parsing_canvas_course_usage_data Call")
        new_dic = {}
        if response is None:
            logger.info(f"For Canvas Course usage response is None ")
            return

        logger.info(f"CanvasCourseUsage data collected so far : {len(self.canvas_usage_courses)}")
        status = response.result().status_code

        course_id = response.result().url.split('courses/')[1].split('/')[0]

        status_codes_to_retry = [500]
        if status != 200:
            logger.info(f'retry_count: {self.retry_count}')
            if self.retry_count < self.retry_attempts and status in status_codes_to_retry:
                logger.info("Append to retry list")
                self.course_retry_list.append(course_id)
            logger.info(f"Response not successful with status code {status} due to {response.result().text}")
            return

        try:
            analytics_data = json.loads(response.result().text)
        except JSONDecodeError as e:
            logger.error(f"Error in parsing the response due to {e.msg}")
            logger.info("Append to retry list")
            self.course_retry_list.append(course_id)
            return

        if not analytics_data:
            logger.info(f"Response for fetching canvas course usage is empty")
            return
        new_dic['course_id'] = course_id
        new_dic['analytics'] = analytics_data

        self.canvas_usage_courses.append(new_dic)

    def _get_canvas_course_views_participation_data(self, retry_courses=None):
        logger.info("Starting of _get_canvas_course_views_participation_data call")
        with FuturesSession() as session:
            headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + self.canvas_token[0]}
            # https://umich.instructure.com/api/v1/courses/course_id/analytics/activity
            if retry_courses is None:
                logger.info("Initial round getting canvas_course_usage data")
                responses = [session.get(f'{self.canvas_url[0]}/api/v1/courses/{course_id}/analytics/activity',
                                         headers=headers) for course_id in self.course_ids]
            else:
                self.course_retry_list = []
                logger.info("Retry round getting canvas_course_usage data")
                responses = [session.get(f'{self.canvas_url[0]}/api/v1/courses/{retry_course}/analytics/activity',
                                         headers=headers) for retry_course in retry_courses]

            for response in as_completed(responses):
                self.parsing_canvas_course_usage_data(response)

        logger.info(f"Any thing to Retry? With List of length {len(self.course_retry_list)} : {self.course_retry_list}")
        if len(self.course_retry_list) != 0 and self.retry_count < self.retry_attempts:
            logger.info(f"Retrying again for {self.retry_count + 1}")
            self.retry_count = self.retry_count + 1
            self._get_canvas_course_views_participation_data(self.course_retry_list)

    # preparing the data to be loaded to df in format [date, views, paticipations, course_id]
    def canvas_course_usage_to_df(self):
        rows = []
        for data in self.canvas_usage_courses:
            analytics = data['analytics']
            course_id = data['course_id']

            for row in analytics:
                row['course_id'] = course_id
                rows.append(row)

        df = pd.DataFrame(rows)
        logger.info(df.head())
        df.drop(['id'], axis=1, inplace=True)
        df_dup = df[df.duplicated()]
        logger.info('Check for duplicate items ')
        logger.info(df_dup)
        df = df.drop_duplicates()
        logger.info(df.head())
        return df

    def get_canvas_course_views_participation_data(self):
        start = time.time()
        self._get_canvas_course_views_participation_data()
        delta = time.time() - start
        str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
        logger.info(f'Duration of Canvas Course usage run took: {str_time}')
        return self.canvas_course_usage_to_df()