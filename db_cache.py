# standard libraries
import logging, json, os, re
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import pandas as pd
from sqlalchemy import create_engine
from umich_api.api_utils import ApiUtil


# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_CACHE_PATH_ELEMS = ENV['DB_CACHE_PATH']
DB_CACHE_PATH_STR = '/'.join(DB_CACHE_PATH_ELEMS)
CACHE_ENGINE = create_engine(f'sqlite:///{DB_CACHE_PATH_STR}')

API_UTIL = ApiUtil(ENV['API_BASE_URL'], ENV['API_CLIENT_ID'], ENV['API_CLIENT_SECRET'])
SUBSCRIPTION_NAME = ENV['API_SUBSCRIPTION_NAME']


# Functions - Requests and Caching

# Create unique request string for WorldCat Search API caching
def create_unique_request_str(base_url: str, params_dict: Dict[str, str]) -> str:
    sorted_params = sorted(params_dict.keys())
    fields = []
    for param in sorted_params:
        value = params_dict[param]
        if isinstance(value, list):
            value = f'{",".join(value)}'
        fields.append('{}-{}'.format(param, value))
    return base_url + '?' + '&'.join(fields)


# Make the request and cache new data, or retrieves the cached data
def make_request_using_cache(url: str, params: Dict[str, str] = {}) -> Sequence[Dict]:
    unique_req_url = create_unique_request_str(url, params)
    logger.debug(f'Unique Request URL: {unique_req_url}')
    cache_df = pd.read_sql(f'''
        SELECT * FROM request WHERE request_url = '{unique_req_url}';
    ''', CACHE_ENGINE)

    if not cache_df.empty:
        logger.debug('Retrieving cached data...')
        record_series = cache_df.iloc[0]
        return json.loads(record_series['response'])

    logger.debug('Making a request for new data...')
    response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params)
    logger.info('Received response with the following URL: ' + response.url)

    status_code = response.status_code
    if status_code != 200:
        logger.debug(response.text)
        logger.warning(f'Received irregular status code: {status_code}')
        return ''

    new_request_df = pd.DataFrame({
        'request_url': [unique_req_url],
        'response': [response.text],
        'timestamp': datetime.now().isoformat()
    })

    new_request_df.to_sql('request', CACHE_ENGINE, if_exists='append', index=False)
    response_data = json.loads(response.text)
    return response_data


# Functions - DB

def init_db() -> None:
    try:
        conn = CACHE_ENGINE.connect()
        conn.close()
        logger.info(f'Created or connected to {DB_CACHE_PATH_STR} database')
    except:
        logger.error(f'Unable to create or connect to {DB_CACHE_PATH_STR} database')


def create_table(table_name: str, create_statement: str) -> None:
    conn = CACHE_ENGINE.connect()
    drop_statement = f'''DROP TABLE IF EXISTS '{table_name}';'''
    conn.execute(drop_statement)
    logger.info(f'Dropped table {table_name}, if it existed.')
    conn.execute(create_statement)
    logger.info(f'Created table {table_name} in {DB_CACHE_PATH_STR}')
    conn.close()


def set_up_database() -> None:
    init_db()

    request_create_statement = '''
        CREATE TABLE 'request' (
            'request_id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            'request_url' TEXT NOT NULL UNIQUE,
            'response' BLOB NOT NULL,
            'timestamp' TEXT NOT NULL
        );
    '''
    create_table('request', request_create_statement)


# Main Program

if __name__ == '__main__':
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    set_up_database()
