# standard libraries
import json, logging, os
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Union
import time

# third-party libraries
import psycopg2, pytz
import pandas as pd
from requests import Response
from umich_api.api_utils import ApiUtil

# local libraries
from db.db_creator import DBCreator
from canvas.published_date import FetchPublishedDate


# Initialize settings and globals

logger = logging.getLogger(__name__)

try:
    config_path = os.getenv("ENV_PATH", os.path.join('config', 'secrets', 'env.json'))
    with open(config_path) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ACCOUNT_ID = ENV.get('CANVAS_ACCOUNT_ID', 1)
TERM_ID = ENV['CANVAS_TERM_ID']

API_UTIL = ApiUtil(ENV['API_BASE_URL'], ENV['API_CLIENT_ID'], ENV['API_CLIENT_SECRET'])
SUBSCRIPTION_NAME = ENV['API_SUBSCRIPTION_NAME']
API_SCOPE_PREFIX = ENV['API_SCOPE_PREFIX']
MAX_REQ_ATTEMPTS = ENV['MAX_REQ_ATTEMPTS']
CANVAS_TOKEN = ENV['CANVAS_TOKEN']
CANVAS_URL = ENV['CANVAS_URL']
NUM_ASYNC_WORKERS = ENV.get('NUM_ASYNC_WORKERS', 8)

UDW_CONN = psycopg2.connect(**ENV['UDW'])
WAREHOUSE_INCREMENT = ENV['WAREHOUSE_INCREMENT']

CREATE_CSVS = ENV.get('CREATE_CSVS', False)
INVENTORY_DB = ENV['INVENTORY_DB']
APPEND_TABLES_NAMES = ENV.get('APPEND_TABLE_NAMES', ['job_run'])

# Function(s)


def make_request_using_api_utils(url: str, params: Dict[str, Union[str, int]] = {}) -> Response:
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
                return response
            except JSONDecodeError:
                logger.warning('JSONDecodeError encountered')
                logger.info('Beginning next attempt')

    logger.error('The maximum number of request attempts was reached')
    return response


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
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/accounts/{account_id}/courses'
    params = {
        'with_enrollments': True,
        'enrollment_type': ['student', 'teacher'],
        'enrollment_term_id': term_id,
        'per_page': 100
    }

    # Make first course request
    page_num = 1
    logger.info(f'Course Page Number: {page_num}')
    response = make_request_using_api_utils(url_ending_with_scope, params)
    all_course_data = json.loads(response.text)
    slim_course_dicts = slim_down_course_data(all_course_data)
    more_pages = True

    while more_pages:
        next_params = API_UTIL.get_next_page(response)
        if next_params:
            page_num += 1
            logger.info(f'Course Page Number: {page_num}')
            response = make_request_using_api_utils(url_ending_with_scope, next_params)
            all_course_data = json.loads(response.text)
            slim_course_dicts += slim_down_course_data(all_course_data)
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
        WHERE e.course_id IN ({courses_string})
            AND e.workflow_state='active';
    '''
    logger.info('Making enrollment_dim query')
    enrollment_df = pd.read_sql(enrollment_query, UDW_CONN)
    logger.debug(enrollment_df.head())
    return enrollment_df


def process_sis_id(id: str) -> Union[int, None]:
    try:
        sis_id = int(id)
    except ValueError:
        logger.debug(f'Invalid sis_id found: {id}')
        sis_id = None
    return sis_id


def pull_user_data_from_udw(user_ids: Sequence[int]) -> pd.DataFrame:
    users_string = ','.join([str(user_id) for user_id in user_ids])
    user_query = f'''
        SELECT u.id AS warehouse_id,
               u.canvas_id AS canvas_id,
               u.name AS name,
               p.sis_user_id AS sis_id,
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
    user_df['sis_id'] = user_df['sis_id'].map(process_sis_id, na_action='ignore')
    user_df = user_df.drop_duplicates(subset=['warehouse_id', 'canvas_id'])
    logger.debug(user_df.head())
    return user_df


def check_if_valid_user_id(id: int, user_ids: Sequence[int]) -> bool:
    if id in user_ids:
        return True
    else:
        return False


def run_course_inventory() -> None:
    start = time.time()

    # Gather course data
    course_df = gather_course_info_for_account(ACCOUNT_ID, TERM_ID)
    course_available_df = course_df.loc[course_df.workflow_state == 'available'].copy()
    course_available_ids = course_available_df['canvas_id'].to_list()
    logger.info("**** Fetching the Published date ***")
    published_dates = FetchPublishedDate(CANVAS_URL, CANVAS_TOKEN, NUM_ASYNC_WORKERS, course_available_ids)
    published_course_date = published_dates.get_published_course_date(course_available_ids)
    course_published_date_df = pd.DataFrame(published_course_date.items(), columns=['canvas_id','published_at'])
    course_df = pd.merge(course_df, course_published_date_df, on='canvas_id', how='left')
    logger.info("*** Checking for courses available and no published date ***")
    logger.info(course_df[(course_df['workflow_state'] == 'available') & (course_df['published_at'].isnull())])
    course_df['created_at'] = pd.to_datetime(course_df['created_at'],
                                             format="%Y-%m-%dT%H:%M:%SZ",
                                             errors='coerce')
    course_df['published_at'] = pd.to_datetime(course_df['published_at'],
                                               format="%Y-%m-%dT%H:%M:%SZ",
                                               errors='coerce')

    # Gather enrollment data
    udw_course_ids = course_df['warehouse_id'].to_list()
    enrollment_df = pull_enrollment_data_from_udw(udw_course_ids)

    # Gather user data
    udw_user_ids = enrollment_df['user_id'].drop_duplicates().to_list()
    user_df = pull_user_data_from_udw(udw_user_ids)

    # Find and remove rows with nonexistent user ids from enrollment_df
    # This can take a few minutes
    logger.info('Looking for rows with nonexistent user ids in enrollment data')
    valid_user_ids = user_df['warehouse_id'].to_list()
    enrollment_df['valid_id'] = enrollment_df['user_id'].map(
        lambda x: check_if_valid_user_id(x, valid_user_ids)
    )
    enrollment_df = enrollment_df[(enrollment_df['valid_id'])]
    enrollment_df = enrollment_df.drop(columns=['valid_id'])

    num_course_records = len(course_df)
    num_user_records = len(user_df)
    num_enrollment_records = len(enrollment_df)

    if CREATE_CSVS:
        # Generate CSV Output
        logger.info(f'Writing {num_course_records} course records to CSV')
        course_df.to_csv(os.path.join('data', 'course.csv'), index=False)
        logger.info('Wrote data to data/course.csv')
        logger.info(f'Writing {num_user_records} user records to CSV')
        user_df.to_csv(os.path.join('data', 'user.csv'), index=False)
        logger.info('Wrote data to data/user.csv')
        logger.info(f'Writing {num_enrollment_records} enrollment records to CSV')
        enrollment_df.to_csv(os.path.join('data', 'enrollment.csv'), index=False)
        logger.info('Wrote data to data/enrollment.csv')

    # Empty tables (if any) in database, then migrate
    logger.info('Emptying tables in DB')
    db_creator_obj = DBCreator(INVENTORY_DB, APPEND_TABLES_NAMES)
    db_creator_obj.set_up_database()

    # Insert gathered data
    logger.info(f'Inserting {num_course_records} course records to DB')
    course_df.to_sql('course', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into course table in {db_creator_obj.db_name}')
    logger.info(f'Inserting {num_user_records} user records to DB')
    user_df.to_sql('user', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into user table in {db_creator_obj.db_name}')
    logger.info(f'Inserting {num_enrollment_records} enrollment records to DB')
    enrollment_df.to_sql('enrollment', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into enrollment table in {db_creator_obj.db_name}')

    # Add record to job_run table
    utc_now = time.gmtime(time.time())
    now_mysql_datetime = time.strftime('%Y-%m-%d %H:%M:%S', utc_now)
    job_run_df = pd.DataFrame({'timestamp': [now_mysql_datetime]})
    job_run_df.to_sql('job_run', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted job_run record with UTC timestamp of {now_mysql_datetime}')

    delta = time.time() - start
    str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
    logger.info(f'Duration of run: {str_time}')


if __name__ == "__main__":
    run_course_inventory()
