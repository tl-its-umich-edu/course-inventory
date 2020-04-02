import logging
import time
import pandas as pd
from json.decoder import JSONDecodeError
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import json
logger = logging.getLogger(__name__)


class CanvasUsage:
    def __init__(self, canvas_url, canvas_token, retry_attempts, canvas_ids):
        self.canvas_url = canvas_url,
        self.canvas_token = canvas_token,
        self.canvas_ids = canvas_ids
        self.retry_attempts = retry_attempts
        self.canvas_usage_courses = []
        self.course_retry_list = []
        self.retry_count = 0

    def parsing_canvas_usage_data(self, response):
        logger.info("parsing_canvas_usage_data Call")
        new_dic = {}
        if response is None:
            logger.info(f"For Canvas usage response is None ")
            return

        logger.info(f"CanvasUsage date collected so far : {len(self.canvas_usage_courses)}")
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
            return

        if not analytics_data:
            logger.info(f"Response for fetching canvas usage is empty {analytics_data}")
            return
        new_dic['canvas_id'] = course_id
        new_dic['analytics'] = analytics_data

        self.canvas_usage_courses.append(new_dic)

    def _get_canvas_views_participation_data(self, retry_courses=None):
        logger.info("Starting of get_canvas_usage call")
        with FuturesSession() as session:
            headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + self.canvas_token[0]}
            # https://umich.instructure.com/api/v1/courses/course_id/analytics/activity
            if retry_courses is None:
                logger.info("Initial round getting canvas_usage data")
                responses = [session.get(f'{self.canvas_url[0]}/api/v1/courses/{course_id}/analytics/activity',
                                         headers=headers) for course_id in self.canvas_ids]
            else:
                self.course_retry_list = []
                logger.info("Retry round getting canvas_usage")
                responses = [session.get(f'{self.canvas_url[0]}/api/v1/courses/{retry_course}/analytics/activity',
                                         headers=headers) for retry_course in retry_courses]

            for response in as_completed(responses):
                self.parsing_canvas_usage_data(response)

        logger.info(f"Any thing to Retry? With List of length {len(self.course_retry_list)} : {self.course_retry_list}")
        if len(self.course_retry_list) != 0 and self.retry_count < self.retry_attempts:
            logger.info(f"Retrying again for {self.retry_count + 1}")
            self.retry_count = self.retry_count + 1
            self._get_canvas_views_participation_data(self.course_retry_list)

    # preparing the data to be loaded to df in format [date, views, paticipations, canvas_id]
    def canvas_usage_to_df(self):
        rows = []
        for data in self.canvas_usage_courses:
            analytics = data['analytics']
            canvas_id = data['canvas_id']

            for row in analytics:
                row['canvas_id'] = canvas_id
                rows.append(row)

        df = pd.DataFrame(rows)
        df.drop(['id'], axis=1, inplace=True)
        df_dup = df[df.duplicated()]
        logger.info('Check for duplicate items ')
        logger.info(df_dup)
        df = df.drop_duplicates()
        logger.info(df.head())
        return df

    def get_canvas_views_participation_data(self):
        start = time.time()
        self._get_canvas_views_participation_data()
        delta = time.time() - start
        str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
        logger.info(f'Duration of Canvas usage run took: {str_time}')
        return self.canvas_usage_to_df()