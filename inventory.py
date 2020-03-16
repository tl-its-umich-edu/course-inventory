# standard libraries
import json, logging, os
from json.decoder import JSONDecodeError
from typing import Dict, Sequence, Union

# third-party libraries
import pandas as pd
import psycopg2
from umich_api.api_utils import ApiUtil


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
PER_PAGE_COUNT = ENV['PER_PAGE_COUNT']

UDW_CONN = psycopg2.connect(**ENV['UDW'])
WAREHOUSE_INCREMENT = ENV['WAREHOUSE_INCREMENT']


# Function(s)

def make_audit_logs_api_request(url, params=None):
    logger.info(f"make_audit_logs_api_request url: {url}")
    response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params)
    return response


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
            'course_id': course_dict['id'],
            'course_name': course_dict['name'],
            'course_account_id': course_dict['account_id'],
            'course_created_at': course_dict['created_at'],
            'course_workflow_state': course_dict['workflow_state']
        }
        slim_course_dicts.append(slim_course_dict)
    return slim_course_dicts

def get_next_page_url(response):
    """
    get the next page url from the Http response headers
    :param response:
    :type response: requests
    :return: next_page_url
    :rtype: str
    """
    logging.debug(get_next_page_url.__name__ + '() called')

    if not response.links:
        logging.debug('The api call do not have Link headers')
        return None

    for page in response.links:
        if 'next' in page:
            return response.links['next']['url']


def handle_request_if_failed(response):
    if response.status_code != 200:
        return False
    else:
        return True


def get_course_publish_unpublish_date(df_row: str, publish_or_unpublish_date, next_page_url = None)->str:
    logger.debug(df_row)
    if df_row['course_workflow_state'] == 'available':
        event_type_to_check_for = 'published'
    if df_row['course_workflow_state'] == 'unpublished':
        event_type_to_check_for = 'claimed'

    course_id= df_row['course_id']

    if next_page_url is not None:
        url = next_page_url
    else:
        url = f"{API_SCOPE_PREFIX}/audit/course/courses/{course_id}?per_page={PER_PAGE_COUNT}"

    try:
        response = make_audit_logs_api_request(url)

    except (Exception) as e:
        logger.exception('getting sections has erroneous response ' + e.message)
        return publish_or_unpublish_date

    if not handle_request_if_failed(response):
        return publish_or_unpublish_date

    audit_events = json.loads(response.text.encode('utf8'))
    if not audit_events:
        return publish_or_unpublish_date

    events = audit_events['events']

    for event in events:
        if event['event_type'] == event_type_to_check_for:
            publish_or_unpublish_date = event['created_at']
            logger.info(f"Date for Workflow type :{df_row['course_workflow_state']} for course {course_id}")
            break
    if publish_or_unpublish_date is None:
        next_page_url = get_next_page_url(response)
        if next_page_url is not None:
            get_course_publish_unpublish_date(course_id, publish_or_unpublish_date, next_page_url)

    logger.info(f"For course {course_id} publish_or_unpublish_date: {publish_or_unpublish_date}")
    if publish_or_unpublish_date is None:
        logger.info(f"For course {course_id} for workflow type {df_row['course_workflow_state']} don't have any date")
        return 'null'

    return publish_or_unpublish_date


def gather_course_info_for_account(account_id: int, term_id: int) -> Sequence[int]:
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
    course_df['course_warehouse_id'] = course_df['course_id'].map(lambda x: x + WAREHOUSE_INCREMENT)
    course_date=None
    course_df['unpub_published_date'] = course_df.apply(lambda x: get_course_publish_unpublish_date(x, course_date), axis=1)
    logger.debug(course_df.head())
    course_df.to_csv(os.path.join('data', 'course.csv'), index=False)
    logger.info('Course data was written to data/course.csv')
    course_ids = course_df['course_warehouse_id'].to_list()
    return course_ids


def pull_enrollment_and_user_data(udw_course_ids) -> None:
    udw_courses_string = ','.join([str(udw_course_id) for udw_course_id in udw_course_ids])
    enrollment_query = f'''
        SELECT e.id AS enrollment_id,
               e.canvas_id AS enrollment_canvas_id,
               e.user_id AS enrollment_user_id,
               e.course_section_id AS enrollment_course_section_id,
               e.course_id AS enrollment_course_id,
               e.workflow_state AS enrollment_workflow_state,
               r.base_role_type AS role_type
        FROM enrollment_dim e
        JOIN role_dim r
            ON e.role_id=r.id
        WHERE e.course_id IN ({udw_courses_string});
    '''

    logger.info('Making enrollment_dim query')
    enrollment_df = pd.read_sql(enrollment_query, UDW_CONN)

    enrollment_df.to_csv(os.path.join('data', 'enrollment.csv'), index=False)
    logger.info('Enrollment data was written to data/enrollment.csv')

    user_ids = enrollment_df['enrollment_user_id'].drop_duplicates().to_list()
    users_string = ','.join([str(user_id) for user_id in user_ids])

    user_query = f'''
        SELECT u.id AS user_id,
               u.canvas_id AS user_canvas_id,
               u.name AS user_name,
               u.workflow_state AS user_workflow_state,
               p.unique_name AS pseudonym_uniqname
        FROM user_dim u
        JOIN pseudonym_dim p
            ON u.id=p.user_id
        WHERE u.id in ({users_string});
    '''

    logger.info('Making user_dim query')
    user_df = pd.read_sql(user_query, UDW_CONN)
    user_df.to_csv(os.path.join('data', 'user.csv'), index=False)
    logger.info('User data was written to data/user.csv')


if __name__ == "__main__":
    current_udw_course_ids = gather_course_info_for_account(1, ENV['TERM_ID'])
    pull_enrollment_and_user_data(current_udw_course_ids)
