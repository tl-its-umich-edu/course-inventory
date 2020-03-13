# standard libraries
import json, logging, os
from typing import Dict, Sequence

# third-party libraries
import pandas as pd

# local libraries
from db_cache import make_request_using_cache


# Initialize settings and globals

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

API_BASE_URL = ENV['API_BASE_URL']
API_KEY = ENV['API_KEY']


# Function(s)

def slim_down_course_data(course_data: Sequence[Dict]) -> Sequence[Dict]:
    slim_course_dicts = []
    for course_dict in course_data:
        slim_course_dict = {
            'id': course_dict['id'],
            'name': course_dict['name'],
            'account_id': course_dict['account_id'],
            'created_at': course_dict['created_at'],
            'workflow_state': course_dict['workflow_state']
        }
        slim_course_dicts.append(slim_course_dict)
    return slim_course_dicts


def gather_course_info_for_account(account_id: int, term_id: int) -> Sequence[int]:
    url_ending = f'accounts/{account_id}/courses'
    params = {
        'access_token': API_KEY,
        'with_enrollments': True,
        'enrollment_type': ['student', 'teacher'],
        'enrollment_term_id': term_id,
        'per_page': 100,
        'page': 1
    }

    slim_course_dicts = []
    more_pages = True
    while more_pages:
        logger.info(f"Course Page Number: {params['page']}")
        all_course_data = make_request_using_cache(API_BASE_URL + url_ending, params)
        if len(all_course_data) > 0:
            slim_course_dicts += slim_down_course_data(all_course_data)
            params['page'] += 1
        else:
            logger.info('No more pages!')
            more_pages = False

    course_df = pd.DataFrame(slim_course_dicts)
    logger.info(course_df.head())
    logger.info(len(course_df))
    course_df.to_csv(os.path.join('data', 'course.csv'))
    course_ids = course_df['id'].to_list()
    return course_ids


if __name__ == "__main__":
    course_ids = gather_course_info_for_account(1, ENV['TERM_ID'])
    # Use course_ids to find instructor enrollments
    # Find related accounts by using a different endpoint
