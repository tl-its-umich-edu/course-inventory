# standard libraries
import json, logging, os, time
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Tuple, Union

# third-party libraries
import pandas as pd
import psycopg2
import requests
from requests import Response
from umich_api.api_utils import ApiUtil

# local libraries
from db.db_creator import DBCreator
from canvas.published_date import FetchPublishedDate
from gql_queries import queries as QUERIES


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

CREATE_CSVS = ENV.get('CREATE_CSVS', False)
INVENTORY_DB = ENV['INVENTORY_DB']


# Function(s)

def make_request_using_lib(
    url: str,
    params: Dict[str, Union[str, int]] = {},
    method: str = 'GET',
    lib_name: str = 'requests'
) -> Response:

    logger.debug('Making a request for data...')

    for i in range(1, MAX_REQ_ATTEMPTS + 1):
        logger.debug(f'Attempt #{i}')
        if lib_name == 'requests':
            response = requests.request(method=method, url=url, json=params)
        elif lib_name == 'umich_api':
            response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params, method=method)
        else:
            logger.error('lib_name provided was invalid!')
        logger.debug('Received response with the following URL: ' + response.url)
        status_code = response.status_code

        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            logger.debug(response.text)
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


# Functions - Course

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
        if 'total_students' in course_dict.keys():
            slim_course_dict['total_students'] = int(course_dict['total_students'])
        else:
            logger.info('Total students not found, setting to zero')
            slim_course_dict['total_students'] = 0
            logger.info(course_dict['total_students'])
        slim_course_dicts.append(slim_course_dict)
    return slim_course_dicts


def gather_course_data_from_api(account_id: int, term_id: int) -> pd.DataFrame:
    logger.info('** gather_course_data_from_api')
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/accounts/{account_id}/courses'
    params = {
        'with_enrollments': True,
        'enrollment_type': ['student', 'teacher'],
        'enrollment_term_id': term_id,
        'per_page': 100,
        'include': ['total_students']
    }

    # Make first course request
    page_num = 1
    logger.info(f'Course Page Number: {page_num}')
    response = make_request_using_lib(
        url_ending_with_scope,
        params,
        method='GET',
        lib_name='umich_api'
    )
    all_course_data = json.loads(response.text)
    slim_course_dicts = slim_down_course_data(all_course_data)
    more_pages = True

    while more_pages:
        next_params = API_UTIL.get_next_page(response)
        if next_params:
            page_num += 1
            logger.info(f'Course Page Number: {page_num}')
            response = make_request_using_lib(
                url_ending_with_scope,
                next_params,
                method='GET',
                lib_name='umich_api'
            )
            all_course_data = json.loads(response.text)
            slim_course_dicts += slim_down_course_data(all_course_data)
        else:
            logger.info('No more pages!')
            more_pages = False

    num_slim_course_dicts = len(slim_course_dicts)
    logger.info(f'Total course records: {num_slim_course_dicts}')
    slim_course_dicts_with_students = []
    for slim_course_dict in slim_course_dicts:
        if slim_course_dict['total_students'] > 0:
            slim_course_dicts_with_students.append(slim_course_dict)
    num_slim_course_dicts_with_students = len(slim_course_dicts_with_students)

    logger.info(f'Course records with students: {num_slim_course_dicts_with_students}')
    logger.info(f'Dropped {num_slim_course_dicts - num_slim_course_dicts_with_students} records')

    course_df = pd.DataFrame(slim_course_dicts_with_students)
    course_df.to_csv(os.path.join('data', 'course_with_total_students.csv'), index=False)
    course_df = course_df.drop(['total_students'], axis='columns')
    logger.debug(course_df.head())
    return course_df


# Functions - Enrollment

def unnest_enrollments(enroll_dict: Dict) -> Tuple[Dict, ...]:
    flat_enroll_dict = {
        'canvas_id': int(enroll_dict['_id']),
        'user_id': int(enroll_dict['user']['_id']),
        'course_id': int(enroll_dict['course']['_id']),
        'course_section_id': int(enroll_dict['section']['_id']),
        'role_type': enroll_dict['type'],
        'workflow_state': enroll_dict['state']
    }

    user_data = enroll_dict['user']
    flat_user_dict = {
        'canvas_id': int(user_data['_id']),
        'name': user_data['name']
    }

    section_data = enroll_dict['section']
    flat_section_dict = {
        'canvas_id': int(section_data['_id']),
        'name': section_data['name'],
    }
    return (flat_enroll_dict, flat_user_dict, flat_section_dict)


def gather_enrollment_data_with_graphql(course_ids: Sequence[int]) -> Tuple[pd.DataFrame, ...]:
    logger.info('** gather_enrollment_data_with_graphql')
    start = time.time()

    complete_url = CANVAS_URL + '/api/graphql'
    course_enrollments_query = QUERIES['course_enrollments']
    params = {
        'access_token': CANVAS_TOKEN,
        'query': course_enrollments_query,
        'variables': {
            "courseID": None,
            "enrollmentPageSize": 75,
            "enrollmentPageCursor": "",
        }
    }

    enrollment_records = []
    user_records = []
    section_records = []

    course_num = 0
    for course_id in course_ids:
        course_num += 1
        params['variables']['courseID'] = course_id

        logger.info(f'Enrollment records: {len(enrollment_records)}')
        logger.info(f'Course number {course_num}: {course_id}')

        more_enroll_pages = True
        enroll_page_num = 0
        while more_enroll_pages:
            enroll_page_num += 1
            logger.info(f'Enrollment Page Number: {enroll_page_num}')

            response = make_request_using_lib(
                complete_url,
                params,
                method='POST',
                lib_name='requests'
            )
            data = json.loads(response.text)

            response_course_id = data['data']['course']['_id']

            enrollments_connection = data['data']['course']['enrollmentsConnection']
            enrollment_dicts = enrollments_connection['nodes']
            enrollment_page_info = enrollments_connection['pageInfo']

            for enrollment_dict in enrollment_dicts:
                enrollment_record, user_record, section_record = unnest_enrollments(enrollment_dict)
                enrollment_records.append(enrollment_record)
                user_records.append(user_record)
                section_records.append(section_record)

            if not enrollment_page_info['hasNextPage']:
                more_enroll_pages = False
                params['variables']['enrollmentPageCursor'] = ""
            else:
                enroll_page_cursor = enrollment_page_info['endCursor']
                params['variables']['enrollmentPageCursor'] = enroll_page_cursor

        if course_num % 1000 == 0:
            delta = time.time() - start
            logger.info('** 1000 interval **')
            logger.info(f'Seconds elapsed: {delta}')

    enrollment_df = pd.DataFrame(enrollment_records)
    user_df = pd.DataFrame(user_records).drop_duplicates(subset=['canvas_id'])
    section_df = pd.DataFrame(section_records).drop_duplicates(subset=['canvas_id'])

    delta = time.time() - start
    logger.info(delta)
    return (enrollment_df, user_df, section_df)


def pull_sis_user_data_from_udw(user_ids: Sequence[int]) -> pd.DataFrame:
    udw_conn = psycopg2.connect(**ENV['UDW'])
    users_string = ','.join([str(user_id) for user_id in user_ids])
    user_query = f'''
        SELECT u.canvas_id AS canvas_id,
               p.sis_user_id AS sis_id,
               p.unique_name AS uniqname
        FROM user_dim u
        JOIN pseudonym_dim p
            ON u.id=p.user_id
        WHERE u.canvas_id in ({users_string});
    '''
    logger.info('Making user_dim query')
    udw_user_df = pd.read_sql(user_query, udw_conn)
    udw_user_df['sis_id'] = udw_user_df['sis_id'].map(process_sis_id, na_action='ignore')
    # Found that the IDs are not necessarily unique, so dropping duplicates
    udw_user_df = udw_user_df.drop_duplicates(subset=['canvas_id'])
    logger.debug(udw_user_df.head())
    udw_conn.close()
    return udw_user_df


def process_sis_id(id: str) -> Union[int, None]:
    try:
        sis_id = int(id)
    except ValueError:
        logger.debug(f'Invalid sis_id found: {id}')
        sis_id = None
    return sis_id


def check_if_valid_user_id(id: int, user_ids: Sequence[int]) -> bool:
    if id in user_ids:
        return True
    else:
        return False


def run_course_inventory() -> None:
    logger.info("* run_course_inventory")
    start = time.time()

    # Gather course data
    course_df = gather_course_data_from_api(ACCOUNT_ID, TERM_ID)

    logger.info("*** Fetching the published date ***")
    course_available_df = course_df.loc[course_df.workflow_state == 'available'].copy()
    course_available_ids = course_available_df['canvas_id'].to_list()
    published_dates = FetchPublishedDate(CANVAS_URL, CANVAS_TOKEN, NUM_ASYNC_WORKERS, course_available_ids)
    published_course_date = published_dates.get_published_course_date(course_available_ids)
    course_published_date_df = pd.DataFrame(published_course_date.items(), columns=['canvas_id', 'published_at'])
    course_df = pd.merge(course_df, course_published_date_df, on='canvas_id', how='left')

    logger.info("*** Checking for courses available and no published date ***")
    logger.info(course_df[(course_df['workflow_state'] == 'available') & (course_df['published_at'].isnull())])
    course_df['created_at'] = pd.to_datetime(course_df['created_at'],
                                             format="%Y-%m-%dT%H:%M:%SZ",
                                             errors='coerce')
    course_df['published_at'] = pd.to_datetime(course_df['published_at'],
                                               format="%Y-%m-%dT%H:%M:%SZ",
                                               errors='coerce')

    # Gather enrollment, user, and section data
    course_ids = course_df['canvas_id'].to_list()
    enrollment_df, user_df, section_df = gather_enrollment_data_with_graphql(course_ids)

    # Pull SIS user data from Unizin Data Warehouse
    udw_user_ids = user_df['canvas_id'].to_list()
    sis_user_df = pull_sis_user_data_from_udw(udw_user_ids)
    user_df = pd.merge(user_df, sis_user_df, on='canvas_id', how='left')

    # Produce output
    num_course_records = len(course_df)
    num_user_records = len(user_df)
    num_section_records = len(section_df)
    num_enrollment_records = len(enrollment_df)

    if CREATE_CSVS:
        # Generate CSV Output
        logger.info(f'Writing {num_course_records} course records to CSV')
        course_df.to_csv(os.path.join('data', 'course.csv'), index=False)
        logger.info('Wrote data to data/course.csv')

        logger.info(f'Writing {num_user_records} user records to CSV')
        user_df.to_csv(os.path.join('data', 'user.csv'), index=False)
        logger.info('Wrote data to data/user.csv')

        logger.info(f'Writing {num_section_records} section records to CSV')
        section_df.to_csv(os.path.join('data', 'section.csv'), index=False)
        logger.info('Wrote data to data/section.csv')

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

    logger.info(f'Inserting {num_section_records} section records to DB')
    section_df.to_sql('course_section', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into section table in {db_creator_obj.db_name}')

    logger.info(f'Inserting {num_enrollment_records} enrollment records to DB')
    enrollment_df.to_sql('enrollment', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into enrollment table in {db_creator_obj.db_name}')

    delta = time.time() - start
    str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
    logger.info(f'Duration of run: {str_time}')


if __name__ == "__main__":
    run_course_inventory()
