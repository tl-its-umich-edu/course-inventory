# standard libraries
import json, logging, os
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Union
import time

# third-party libraries
import pandas as pd
import psycopg2
from requests import Response
from umich_api.api_utils import ApiUtil
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed

# local libraries
from db.db_creator import DBCreator


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
published_course_date = {}
published_course_next_page_list = []

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


def check_if_valid_user_id(id: int, user_ids: Sequence[int]) -> bool:
    if id in user_ids:
        return True
    else:
        return False


def get_next_page_url(response):
    """
    get the next page url from the Http response headers
    :param response:
    :type response: requests
    :return: next_page_url
    :rtype: str
    """
    logging.debug(get_next_page_url.__name__ + '() called')
    results = response.result().links
    if not results:
        logging.debug('The api call do not have Link headers')
        return None

    if 'next' in results:
        url_ = results['next']['url']
        published_course_next_page_list.append(url_)
        logger.info(f"Pagination size {len(published_course_next_page_list)} for published_at date")


def published_date_resp_parsing(response):
    logger.info("published_date_resp_parsing Call")

    if response is None:
        logger.info(f"Published course date response is None ")
        return

    start_time = time.time()

    logger.info(f"published courses date collected so far : {len(published_course_date)}")
    status = response.result().status_code
    published_date_found = False
    if status != 200:
        logger.info(f"Response not successful with status code {status} due to {response.result().text}")
        return

    try:
        audit_events = json.loads(response.result().text)
    except JSONDecodeError as e:
        logger.info(f"Error in parsing the response {e.message}")
        return

    if not audit_events:
        logger.info(f"Response for fetching published date is empty {audit_events}")
        return

    events = audit_events['events']

    for event in events:
        if event['event_type'] == 'published':
            course_id = event['links']['course']
            published_date_found = True
            published_course_date.update({course_id: event['created_at']})
            logger.info(f"Published Date {event['created_at']} for course {course_id}")
            break
    if not published_date_found:
        get_next_page_url(response)

    seconds = time.time() - start_time
    str_time = time.strftime("%H:%M:%S", time.gmtime(seconds))
    logger.debug(f"Parsing the published date took {str_time} ")
    return


def get_published_course_date(course_ids, next_page_links=None):
    logger.info("Starting of get_published_course_date call")
    with FuturesSession(max_workers=NUM_ASYNC_WORKERS) as session:
        headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + CANVAS_TOKEN}
        if next_page_links is not None:
            logger.info("Going through Next page URL set")
            responses = []
            for next_page_link in next_page_links:
                response = session.get(next_page_link, headers=headers)
                published_course_next_page_list.remove(next_page_link)
                responses.append(response)
        else:
            logger.info("Initial Round of Fetching course published date")
            responses = [
                session.get(f'{CANVAS_URL}/api/v1/audit/course/courses/{course_id}?per_page=100', headers=headers)
                for course_id in course_ids]

        for response in as_completed(responses):
            published_date_resp_parsing(response)

    if len(published_course_next_page_list) != 0:
        logger.info(f"""Pagination size {len(published_course_next_page_list)} with published_at date items 
                         {published_course_next_page_list}""")
        get_published_course_date(course_ids, published_course_next_page_list)


def run_course_inventory() -> None:
    start = time.time()

    # Gather course data
    course_df = gather_course_info_for_account(ACCOUNT_ID, TERM_ID)
    course_available_df = course_df.loc[course_df.workflow_state == 'available'].copy()
    course_available_ids = course_available_df['canvas_id'].to_list()
    get_published_course_date(course_available_ids)
    course_published_date_df = pd.DataFrame(published_course_date.items(), columns=['canvas_id','published_at'])
    course_df = pd.merge(course_df, course_published_date_df, on='canvas_id', how='left')
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
    db_creator_obj = DBCreator(INVENTORY_DB)
    db_creator_obj.set_up()
    if len(db_creator_obj.get_table_names()) > 0:
        db_creator_obj.drop_records()
    db_creator_obj.migrate()
    db_creator_obj.tear_down()

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

    delta = time.time() - start
    str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
    logger.info(f'Duration of run: {str_time}')


if __name__ == "__main__":
    run_course_inventory()
