# standard libraries
import logging, json, os, re
from datetime import datetime
from typing import Dict, Tuple

# third-party libraries
import pandas as pd
import requests
from sqlalchemy import create_engine

# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_CACHE_PATH_ELEMS = ENV['DB_CACHE_PATH']
DB_CACHE_PATH_STR = '/'.join(DB_CACHE_PATH_ELEMS)
ENGINE = create_engine(f'sqlite:///{DB_CACHE_PATH_STR}')


# Functions - Caching

# Create unique request string for WorldCat Search API caching
def create_unique_request_str(base_url: str, params_dict: Dict[str, str], private_keys=['access_token']) -> str:
    sorted_params = sorted(params_dict.keys())
    fields = []
    for param in sorted_params:
        if param not in private_keys:
            value = params_dict[param]
            if isinstance(value, list):
                value = f'{",".join(value)}'
            fields.append('{}-{}'.format(param, value))
    return base_url + '?' + '&'.join(fields)


# Make the request and cache new data, or retrieves the cached data
def make_request_using_cache(url: str, params: Dict[str, str]={}) -> Tuple:
    unique_req_url = create_unique_request_str(url, params)
    logger.info(unique_req_url)
    cache_df = pd.read_sql(f'''
        SELECT * FROM request WHERE request_url = '{unique_req_url}';
    ''', ENGINE)

    if not cache_df.empty:
        logger.debug('Retrieving cached data...')
        record_series = cache_df.iloc[0]
        return json.loads(record_series['response'])

    logger.debug('Making a request for new data...')
    response = requests.get(url, params)
    logger.info('Received response with the following URL: ' + response.url)
    course_data = json.loads(response.text)

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

    logger.info(type(response.headers))
    response_data = json.loads(response.text)
    response_headers = response.headers

    new_request_df.to_sql('request', ENGINE, if_exists='append', index=False)
    return response_data


# Functions - DB

def init_db() -> None:
    try:
        conn = ENGINE.connect()
        conn.close()
        logger.info(f'Created or connected to {DB_CACHE_PATH_STR} database')
    except:
        logger.error(f'Unable to create or connect to {DB_CACHE_PATH_STR} database')


def create_table(table_name: str, create_statement: str) -> None:
    conn = ENGINE.connect()
    drop_statement = f'''DROP TABLE IF EXISTS '{table_name}';'''
    conn.execute(drop_statement)
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
    set_up_database()
