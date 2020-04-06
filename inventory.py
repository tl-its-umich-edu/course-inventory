# standard libraries
import json, logging, os, time
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Union

# third-party libraries
import pandas as pd
import psycopg2
from requests import Response
from umich_api.api_utils import ApiUtil

# local libraries
from db.db_creator import DBCreator
from canvas.published_date import FetchPublishedDate
from canvas.async_enroll_gatherer import AsyncEnrollGatherer
from gql_queries import queries as QUERIES
from canvas.canvas_course_usage import CanvasCourseUsage


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

def make_request_using_api_utils(url: str, params: Dict[str, Union[str, int]] = {}) -> Response:
    logger.debug('Making a request for data...')

    for i in range(1, MAX_REQ_ATTEMPTS + 1):
        logger.debug(f'Attempt #{i}')
        response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params)
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
        if 'total_students' in course_dict.keys():
            slim_course_dict['total_students'] = int(course_dict['total_students'])
        else:
            logger.info('Total students not found, setting to zero')
            slim_course_dict['total_students'] = 0
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
    course_df = course_df.drop(['total_students'], axis='columns')
    logger.debug(course_df.head())
    return course_df


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
    logger.info("*** Fetching the canvas course usage data ****")

    canvas_course_usage = CanvasCourseUsage(CANVAS_URL, CANVAS_TOKEN, MAX_REQ_ATTEMPTS, course_available_ids)
    canvas_course_usage_df = canvas_course_usage.get_canvas_course_views_participation_data()

    # Gather enrollment, user, and section data
    course_ids = course_df['canvas_id'].to_list()

    enroll_start = time.time()
    enroll_gatherer = AsyncEnrollGatherer(
        course_ids=course_ids,
        access_token=CANVAS_TOKEN,
        complete_url=CANVAS_URL + '/api/graphql',
        gql_query=QUERIES['course_enrollments'],
        enroll_page_size=75,
        num_workers=NUM_ASYNC_WORKERS
    )
    enroll_gatherer.gather()
    enrollment_df, user_df, section_df = enroll_gatherer.generate_output()
    enroll_delta = time.time() - enroll_start
    logger.info(f'Duration of process (seconds): {enroll_delta}')

    # Pull SIS user data from Unizin Data Warehouse
    udw_user_ids = user_df['canvas_id'].to_list()
    sis_user_df = pull_sis_user_data_from_udw(udw_user_ids)
    user_df = pd.merge(user_df, sis_user_df, on='canvas_id', how='left')

    # Produce output
    num_course_records = len(course_df)
    num_user_records = len(user_df)
    num_section_records = len(section_df)
    num_enrollment_records = len(enrollment_df)
    num_canvas_usage_records = len(canvas_course_usage_df)

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

        logger.info(f"Writing {num_canvas_usage_records} canvas course usage records to CSV")
        canvas_course_usage_df.to_csv(os.path.join('data', 'canvas_course_usage.csv'), index=False)
        logger.info('Wrote data to data/canvas_course_usage.csv')

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

    logger.info(f"Inserting {num_canvas_usage_records} canvas_course_usage records to DB")
    canvas_course_usage_df.to_sql('canvas_course_usage', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into canvas_course_usage table in {db_creator_obj.db_name}')

    delta = time.time() - start
    str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
    logger.info(f'Duration of run: {str_time}')


if __name__ == "__main__":
    run_course_inventory()
