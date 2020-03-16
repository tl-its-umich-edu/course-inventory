# standard libraries
import json, logging, os
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Union

# third-party libraries
import pandas as pd
import psycopg2
from umich_api.api_utils import ApiUtil

# local libraries
from db.create_db import DBCreator, MYSQL_ENGINE
from db.tables import tables


# Initialize settings and globals

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

API_UTIL = ApiUtil(ENV['API_BASE_URL'], ENV['API_CLIENT_ID'], ENV['API_CLIENT_SECRET'])
SUBSCRIPTION_NAME = ENV['API_SUBSCRIPTION_NAME']
API_SCOPE_PREFIX = ENV['API_SCOPE_PREFIX']
MAX_REQ_ATTEMPTS = ENV['MAX_REQ_ATTEMPTS']

UDW_CONN = psycopg2.connect(**ENV['UDW'])
WAREHOUSE_INCREMENT = ENV['WAREHOUSE_INCREMENT']

CREATE_CSVS = ENV.get('CREATE_CSVS', False)

# Function(s)

def make_request_using_api_utils(url: str, params: Dict[str, Union[str, int]] = {}) -> Sequence[Dict]:
    logger.debug('Making a request for data...')

    for i in range(1, MAX_REQ_ATTEMPTS + 1):
        logger.debug(f'Attempt #{i}')
        response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params)
        logger.info('Received response with the following URL: ' + response.url)
        status_code = response.status_code

        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            logger.info('Beginning next_attempt')
        else:
            try:
                response_data = json.loads(response.text)
                return response_data
            except JSONDecodeError:
                logger.warning('JSONDecodeError encountered')
                logger.info('Beginning next attempt')

    logger.error('The maximum number of reqeust attempts was reached')
    return [{}]


def slim_down_course_data(course_data: Sequence[Dict]) -> Sequence[Dict]:
    slim_course_dicts = []
    for course_dict in course_data:
        slim_course_dict = {
            'canvas_id': course_dict['id'],
            'name': course_dict['name'],
            'account_id': course_dict['account_id'],
            'created_at': course_dict['created_at'],
            'workflow_state': course_dict['workflow_state']
        }
        slim_course_dicts.append(slim_course_dict)
    return slim_course_dicts


def gather_course_info_for_account(account_id: int, term_id: int) -> pd.DataFrame:
    url_ending = f'accounts/{account_id}/courses'
    params = {
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
        all_course_data = make_request_using_api_utils(f'{API_SCOPE_PREFIX}/{url_ending}', params)
        if len(all_course_data) > 0:
            slim_course_dicts += slim_down_course_data(all_course_data)
            params['page'] += 1
        else:
            logger.info('No more pages!')
            more_pages = False

    course_df = pd.DataFrame(slim_course_dicts)
    course_df['warehouse_id'] = course_df['canvas_id'].map(lambda x: x + WAREHOUSE_INCREMENT)
    logger.debug(course_df.head())
    return course_df


def pull_enrollment_data_from_udw(course_ids) -> pd.DataFrame:
    courses_string = ','.join([str(course_id) for course_id in course_ids])
    enrollment_query = f'''
        SELECT e.id AS warehouse_id,
               e.canvas_id AS canvas_id,
               e.course_id AS course_id,
               e.course_section_id AS course_section_id,
               e.user_id AS user_id,
               e.workflow_state AS workflow_state,
               r.base_role_type AS role_type
        FROM enrollment_dim e
        JOIN role_dim r
            ON e.role_id=r.id
        WHERE e.course_id IN ({courses_string});
    '''

    logger.info('Making enrollment_dim query')
    enrollment_df = pd.read_sql(enrollment_query, UDW_CONN)
    logger.debug(enrollment_df.head())
    return enrollment_df


def pull_user_data_from_udw(user_ids: Sequence[int]) -> pd.DataFrame:
    users_string = ','.join([str(user_id) for user_id in user_ids])
    user_query = f'''
        SELECT u.id AS warehouse_id,
               u.canvas_id AS canvas_id,
               u.name AS name,
               p.unique_name AS uniqname,
               u.workflow_state AS workflow_state
        FROM user_dim u
        JOIN pseudonym_dim p
            ON u.id=p.user_id
        WHERE u.id in ({users_string});
    '''
    logger.info('Making user_dim query')
    user_df = pd.read_sql(user_query, UDW_CONN)
    # Found that the IDs are not necessarily unique, so dropping duplicates
    user_df = user_df.drop_duplicates(subset=['warehouse_id', 'canvas_id'])
    logger.debug(user_df.head())
    return user_df


def run_course_inventory() -> None:
    start = datetime.now()

    # Gather course data
    course_df = gather_course_info_for_account(1, ENV['TERM_ID'])

    # Gather enrollment data
    udw_course_ids = course_df['warehouse_id'].to_list()
    enrollment_df = pull_enrollment_data_from_udw(udw_course_ids)

    # Gather user data
    udw_user_ids = enrollment_df['user_id'].drop_duplicates().to_list()
    user_df = pull_user_data_from_udw(udw_user_ids)

    # Find and remove rows with nonexistent user ids from enrollment_df
    # This can take a few minutes
    logger.info('Looking for rows with nonexistent user ids in enrollment data')
    user_ids = user_df['warehouse_id'].drop_duplicates().to_list()

    def check_if_valid_user_id(id: int) -> bool:
        if id in user_ids:
            return True
        else:
            return False

    enrollment_df['valid_id'] = enrollment_df['user_id'].map(check_if_valid_user_id)
    enrollment_df = enrollment_df[(enrollment_df['valid_id'])].drop(columns=['valid_id'])

    if CREATE_CSVS:
        # Generate CSV Output
        course_df.to_csv(os.path.join('data', 'course.csv'), index=False)
        logger.info('Course data was written to data/course.csv')
        user_df.to_csv(os.path.join('data', 'user_df.csv'), index=False)
        logger.info('User data was written to data/user.csv')
        enrollment_df.to_csv(os.path.join('data', 'enrollment.csv'), index=False)
        logger.info('Enrollment data was written to data/enrollment.csv')

    # Reset database
    db_creator_obj = DBCreator('course_inventory', tables)
    db_creator_obj.set_up_database()

    # Insert gathered data
    logger.info('Inserting course data')
    course_df.to_sql('course', MYSQL_ENGINE, if_exists='append', index=False)
    logger.info('Inserting user data')
    user_df.to_sql('user', MYSQL_ENGINE, if_exists='append', index=False)
    logger.info('Inserting enrollment data')
    enrollment_df.to_sql('enrollment', MYSQL_ENGINE, if_exists='append', index=False)

    delta = datetime.now() - start
    logger.info(f'Duration of run: {delta.total_seconds()}')


if __name__ == "__main__":
    run_course_inventory()
