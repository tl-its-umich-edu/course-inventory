import requests
# standard libraries
import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import Dict, Union

# third-party libraries
import pandas as pd
import furl

# Initialize settings and globals

logger = logging.getLogger(__name__)


# Functions
<<<<<<< HEAD
def get_total_page_count(url:str, headers: Dict[str, Union[str, int]] = {}):
  logger.info("get_total_page_count")
  # get the total page count
  total_page_count = 1
  response = requests.request("GET", f"{url}", headers=headers, data = {})
  status_code = response.status_code
  if status_code != 200:
    logger.warning(f'Received irregular status code: {status_code}')
    logger.info('No page at all!')
  else:
      try:
        results = json.loads(response.text.encode('utf8'))
        logger.info(results)
        total_page_count = results['page_count']

        logger.info(f"url ={url} page count={total_page_count}")
      except JSONDecodeError:
        logger.warning('JSONDecodeError encountered')
=======
def get_total_page_count(url: str, headers: Dict[str, Union[str, int]] = {}):
    # get the total page count
    total_page_count = 1
    response = requests.request("GET", f"{url}", headers=headers, data={})
    status_code = response.status_code
    if status_code != 200:
        logger.warning(f'Received irregular status code: {status_code}')
>>>>>>> Fixed up formatting (flake8), moved settings, added vscode config
        logger.info('No page at all!')
    else:
        try:
            results = json.loads(response.text.encode('utf8'))
            total_page_count = results['page_count']
        except JSONDecodeError:
            logger.warning('JSONDecodeError encountered')
            logger.info('No page at all!')
    return total_page_count


# read configurations
try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error(
        'Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

logger.info(ENV)
zoom_token = ENV['ZOOM_TOKEN']

payload = {}

headers = {
  'Authorization': f'Bearer {zoom_token}'
}


def run_report(param_attribute, headers, json_attribute_name):

    url = ENV[f'ZOOM_{param_attribute}_URL']
    params = ENV[f'ZOOM_{param_attribute}_PARAMS']

    # construct param string
    f = furl.furl('')
    f.args = params
    print(f.url)

    # get total page count
    total_page_count = get_total_page_count(f"{url}{f.url}", headers)

    total_df = pd.DataFrame()

    while (params['page_number'] <= total_page_count):
        logger.info(
            f"Page Number: {params['page_number']} out of total page number {total_page_count}")

        # construct param string
        f = furl.furl('')
        f.args = params
        print(f.url)

        response = requests.request(
            "GET", f"{url}{f.url}", headers=headers, data=payload)
        status_code = response.status_code
        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            logger.info('No more pages!')
        else:
            try:
                results = json.loads(response.text.encode('utf8'))

                df = pd.DataFrame(results[json_attribute_name])
                logger.info(df)
                total_df = total_df.append(df)
                logger.info(f'data frame size {len(total_df)}')
                # go retrieve next page
                params['page_number'] += 1
            except JSONDecodeError:
                logger.warning('JSONDecodeError encountered')
                logger.info('No more pages!')

    # output csv file
    output_file_name = f"total_{json_attribute_name}.csv"
    total_df.to_csv(output_file_name)


# run users report
run_report('USERS', users_params, headers, 'users')

# run meetings report
run_report('MEETINGS', headers, 'meetings')

# run webinars report
run_report('WEBINARS', headers, 'webinars')
