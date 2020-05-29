# standard libraries
import json, logging, os, sys, time
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, Sequence, Union

# third-party libraries
import pandas as pd
import psycopg2
from psycopg2.extensions import connection
from requests import Response
from umich_api.api_utils import ApiUtil

# local libraries
from course_inventory.async_enroll_gatherer import AsyncEnrollGatherer
from course_inventory.canvas_course_usage import CanvasCourseUsage
from course_inventory.gql_queries import queries as QUERIES
from course_inventory.published_date import FetchPublishedDate
from db.db_creator import DBCreator
from environ import DATA_DIR, ENV
from vocab import DataSourceStatus, ValidDataSourceName

# Initialize settings and globals

logger = logging.getLogger(__name__)

CANVAS = ENV.get('CANVAS', {})

ACCOUNT_ID = CANVAS.get('CANVAS_ACCOUNT_ID', 1)
TERM_IDS = CANVAS['CANVAS_TERM_IDS']

API_UTIL = ApiUtil(CANVAS['API_BASE_URL'], CANVAS['API_CLIENT_ID'], CANVAS['API_CLIENT_SECRET'])
SUBSCRIPTION_NAME = CANVAS['API_SUBSCRIPTION_NAME']
API_SCOPE_PREFIX = CANVAS['API_SCOPE_PREFIX']
CANVAS_TOKEN = CANVAS['CANVAS_TOKEN']
CANVAS_URL = CANVAS['CANVAS_URL']

MAX_REQ_ATTEMPTS = ENV.get('MAX_REQ_ATTEMPTS', 3)
NUM_ASYNC_WORKERS = ENV.get('NUM_ASYNC_WORKERS', 8)
CREATE_CSVS = ENV.get('CREATE_CSVS', False)

INVENTORY_DB = ENV['INVENTORY_DB']

CANVAS_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


# Function(s) - Canvas

def make_request_using_api_utils(url: str, params: Union[Dict[str, Any], None] = None) -> Response:
    if params is None:
        request_params = {}
    else:
        request_params = params

    logger.debug('Making a request for data...')

    for i in range(1, MAX_REQ_ATTEMPTS + 1):
        logger.debug(f'Attempt #{i}')
        response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=request_params)
        status_code = response.status_code

        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            logger.info('Beginning next_attempt')
        else:
            try:
                json.loads(response.text)
                return response
            except JSONDecodeError:
                logger.warning('JSONDecodeError encountered')
                logger.info('Beginning next attempt')

    logger.error('The maximum number of request attempts was reached')
    logger.error(f'Data could not be gathered from the URL with the ending "{url}"')
    logger.error('The program will exit')
    sys.exit(1)


def gather_term_data_from_api(account_id: int, term_ids: Sequence[int]) -> pd.DataFrame:
    logger.info('** gather_new_term_data_from_api')

    # Fetch data for terms from config
    logger.info(f'Canvas terms specified in config: {term_ids}')
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/accounts/{account_id}/terms/'

    term_dicts = []
    for term_id in term_ids:
        logger.info(f'Pulling data for term number {term_id}')
        term_url_ending = url_ending_with_scope + str(term_id)
        response = make_request_using_api_utils(term_url_ending)

        term_data = json.loads(response.text)
        slim_term_dict = {
            'canvas_id': term_data['id'],
            'name': term_data['name'],
            'sis_id': int(term_data['sis_term_id']),
            'start_at': pd.to_datetime(
                term_data['start_at'],
                format=CANVAS_DATETIME_FORMAT
            ),
            'end_at': pd.to_datetime(
                term_data['end_at'],
                format=CANVAS_DATETIME_FORMAT
            )
        }
        term_dicts.append(slim_term_dict)

    term_df = pd.DataFrame(term_dicts)
    logger.debug(term_df.head())
    return term_df


def slim_down_course_data(course_data: List[Dict]) -> List[Dict]:
    slim_course_dicts = []
    for course_dict in course_data:
        slim_course_dict = {
            'canvas_id': course_dict['id'],
            'sis_id': course_dict['sis_course_id'],
            'name': course_dict['name'],
            'account_id': course_dict['account_id'],
            'term_id': course_dict['enrollment_term_id'],
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


def gather_course_data_from_api(account_id: int, term_ids: Sequence[int]) -> pd.DataFrame:
    logger.info('** gather_course_data_from_api')
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/accounts/{account_id}/courses'

    course_dicts: List[Dict[str, Any]] = []
    for term_id in term_ids:
        logger.info(f'Fetching course data for term {term_id}')

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
        course_dicts += slim_down_course_data(all_course_data)
        more_pages = True

        while more_pages:
            next_params = API_UTIL.get_next_page(response)
            if next_params:
                page_num += 1
                logger.info(f'Course Page Number: {page_num}')
                response = make_request_using_api_utils(url_ending_with_scope, next_params)
                all_course_data = json.loads(response.text)
                course_dicts += slim_down_course_data(all_course_data)
            else:
                logger.info('No more pages!')
                more_pages = False

    num_course_dicts = len(course_dicts)
    logger.info(f'Total course records for all active terms: {num_course_dicts}')
    course_dicts_with_students = []
    for course_dict in course_dicts:
        if course_dict['total_students'] > 0:
            course_dicts_with_students.append(course_dict)
    num_course_dicts_with_students = len(course_dicts_with_students)

    logger.info(f'Course records with students: {num_course_dicts_with_students}')
    logger.info(f'Dropped {num_course_dicts - num_course_dicts_with_students} course record(s) with no students')

    course_df = pd.DataFrame(course_dicts_with_students)
    course_df = course_df.drop(['total_students'], axis='columns')
    orig_course_count = len(course_df)
    course_df = course_df.drop_duplicates(subset=['canvas_id'], keep='last')
    logger.info(f'Dropped {orig_course_count - len(course_df)} duplicate course record(s)')

    logger.debug(course_df.head())
    return course_df


def gather_account_data_from_api(account_ids: Sequence[int]) -> pd.DataFrame:
    logger.info('** gather_account_data_from_api')
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/accounts/'

    logger.info(f'Fetching account data')
    account_dicts = []
    for account_id in account_ids:
        logger.debug(f'Account number {account_id}')
        account_url_ending = url_ending_with_scope + str(account_id)
        response = make_request_using_api_utils(account_url_ending)
        account_data = json.loads(response.text)
        slim_account_dict = {
            'canvas_id': account_data['id'],
            'name': account_data['name']
        }
        if 'sis_account_id' in account_data.keys():
            slim_account_dict['sis_id'] = account_data['sis_account_id']
        else:
            slim_account_dict['sis_id'] = None
        account_dicts.append(slim_account_dict)
    logger.info('Gathered account data')

    account_df = pd.DataFrame(account_dicts)
    logger.debug(account_df.head())
    return account_df


# Function(s) - UDW

def pull_sis_section_data_from_udw(section_ids: Sequence[int], conn: connection) -> pd.DataFrame:
    section_ids_tup = tuple(section_ids)
    section_query = f'''
        SELECT cs.canvas_id AS canvas_id,
               cs.sis_source_id AS sis_id
        FROM course_section_dim cs
        WHERE cs.canvas_id in %s;
    '''
    logger.info('Making course_section_dim query against UDW')
    udw_section_df = pd.read_sql(section_query, conn, params=(section_ids_tup,))
    logger.debug(udw_section_df.head())
    return udw_section_df


def get_pub_course_info_from_db(db_creator_obj: DBCreator) -> pd.DataFrame:
    logger.info(f"Getting the course info from {db_creator_obj.db_name} database")
    course_from_db_df = pd.read_sql(f'''select canvas_id, published_at from course 
                            where workflow_state = 'available' ;''',
                                    db_creator_obj.engine)
    return course_from_db_df


# Entry point for run_jobs.py


def run_course_inventory() -> Sequence[DataSourceStatus]:
    logger.info("* run_course_inventory")
    # Initialize DBCreator object
    db_creator_obj = DBCreator(INVENTORY_DB)

    logger.info('Making requests against the Canvas API')

    # Gather term data
    term_df = gather_term_data_from_api(ACCOUNT_ID, TERM_IDS)

    # Gather course data
    course_df = gather_course_data_from_api(ACCOUNT_ID, TERM_IDS)
    course_available_df = course_df.loc[course_df.workflow_state == 'available'].copy(deep=True)
    logger.info(f"Size of courses with available workflow state: {course_available_df.shape}")

    course_copy_df = course_df.copy(deep=True)
    course_from_db_df = get_pub_course_info_from_db(db_creator_obj)

    fetch_publish_date = FetchPublishedDate(CANVAS_URL, CANVAS_TOKEN, NUM_ASYNC_WORKERS,
                                            course_copy_df, course_from_db_df, MAX_REQ_ATTEMPTS)
    pub_dates_df = fetch_publish_date.get_published_date()
    course_size_before_merge = course_df.shape[0]
    course_df = pd.merge(course_df, pub_dates_df, on='canvas_id', how='left')
    course_size_after_merge = course_df.shape[0]
    logger.info(
        f"Course info loss due to published date fetch/merge: {course_size_before_merge - course_size_after_merge}")

    course_df['created_at'] = pd.to_datetime(course_df['created_at'],
                                             format=CANVAS_DATETIME_FORMAT,
                                             errors='coerce')

    logger.info("*** Fetching the canvas course usage data ***")
    canvas_course_usage = CanvasCourseUsage(CANVAS_URL, CANVAS_TOKEN, MAX_REQ_ATTEMPTS, course_available_df['canvas_id'].tolist())
    canvas_course_usage_df = canvas_course_usage.get_canvas_course_views_participation_data()

    # Gather account data
    account_ids = sorted(course_df['account_id'].drop_duplicates().to_list())
    account_df = gather_account_data_from_api(account_ids)

    # Gather enrollment and section data
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
    enrollment_df, section_df = enroll_gatherer.generate_output()
    enroll_delta = time.time() - enroll_start
    logger.info(f'Duration of process (seconds): {enroll_delta}')

    # Record data source info for Canvas API
    canvas_data_source = DataSourceStatus(ValidDataSourceName.CANVAS_API)

    udw_conn = psycopg2.connect(**ENV['UDW'])

    # Pull SIS course section data from UDW
    udw_section_ids = section_df['canvas_id'].to_list()
    sis_section_df = pull_sis_section_data_from_udw(udw_section_ids, udw_conn)
    section_df = pd.merge(section_df, sis_section_df, on='canvas_id', how='left')

    # Record data source info for UDW
    udw_meta_df = pd.read_sql('''
        SELECT *
        FROM unizin_metadata
        WHERE key='canvasdatadate';
    ''', udw_conn)
    udw_update_datetime_str = udw_meta_df['value'].iloc[0]
    udw_update_datetime = pd.to_datetime(udw_update_datetime_str, format='%Y-%m-%d %H:%M:%S.%f%z')\
        .to_pydatetime(warn=False)
    logger.info(f'Found canvasdatadate in UDW of {udw_update_datetime}')

    udw_data_source = DataSourceStatus(
        ValidDataSourceName.UNIZIN_DATA_WAREHOUSE, udw_update_datetime)

    # Produce output
    num_term_records = len(term_df)
    num_account_records = len(account_df)
    num_course_records = len(course_df)
    num_section_records = len(section_df)
    num_enrollment_records = len(enrollment_df)
    num_canvas_usage_records = len(canvas_course_usage_df)

    if CREATE_CSVS:
        # Generate CSV output
        logger.info(f'Writing {num_term_records} term records to CSV')
        term_df.to_csv(os.path.join(DATA_DIR, 'term.csv'), index=False)
        logger.info('Wrote data to data/term.csv')

        logger.info(f'Writing {num_account_records} account records to CSV')
        account_df.to_csv(os.path.join(DATA_DIR, 'account.csv'), index=False)
        logger.info('Wrote data to data/account.csv')

        logger.info(f'Writing {num_course_records} course records to CSV')
        course_df.to_csv(os.path.join(DATA_DIR, 'course.csv'), index=False)
        logger.info('Wrote data to data/course.csv')

        logger.info(f'Writing {num_section_records} course_section records to CSV')
        section_df.to_csv(os.path.join(DATA_DIR, 'course_section.csv'), index=False)
        logger.info('Wrote data to data/course_section.csv')

        logger.info(f'Writing {num_enrollment_records} enrollment records to CSV')
        enrollment_df.to_csv(os.path.join(DATA_DIR, 'enrollment.csv'), index=False)
        logger.info('Wrote data to data/enrollment.csv')

        logger.info(f"Writing {num_canvas_usage_records} canvas course usage records to CSV")
        canvas_course_usage_df.to_csv(os.path.join(DATA_DIR, 'canvas_course_usage.csv'), index=False)
        logger.info('Wrote data to data/canvas_course_usage.csv')

    # Empty records from Canvas data tables in database
    logger.info('Emptying Canvas data tables in DB')
    db_creator_obj.drop_records(
        ['account', 'canvas_course_usage', 'course', 'course_section', 'enrollment', 'term']
    )

    # Insert gathered data into DB
    logger.info(f'Inserting {num_term_records} term records to DB')
    term_df.to_sql('term', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into term table in {db_creator_obj.db_name}')

    logger.info(f'Inserting {num_account_records} account records to DB')
    account_df.to_sql('account', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into account table in {db_creator_obj.db_name}')

    logger.info(f'Inserting {num_course_records} course records to DB')
    course_df.to_sql('course', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into course table in {db_creator_obj.db_name}')

    logger.info(f'Inserting {num_section_records} section records to DB')
    section_df.to_sql('course_section', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into section table in {db_creator_obj.db_name}')

    logger.info(f'Inserting {num_enrollment_records} enrollment records to DB')
    enrollment_df.to_sql('enrollment', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into enrollment table in {db_creator_obj.db_name}')

    logger.info(f"Inserting {num_canvas_usage_records} canvas_course_usage records to DB")
    canvas_course_usage_df.to_sql('canvas_course_usage', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into canvas_course_usage table in {db_creator_obj.db_name}')

    return [canvas_data_source, udw_data_source]


# Main Program

if __name__ == "__main__":
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    run_course_inventory()
