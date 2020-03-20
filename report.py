import requests
# standard libraries
import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import Dict, Union
from ratelimit import limits, sleep_and_retry

# third-party libraries
import pandas as pd
import furl

# Initialize settings and globals

logger = logging.getLogger(__name__)


# Functions
def get_total_page_count(url: str, headers: Dict[str, Union[str, int]] = {}):
    # get the total page count
    total_page_count = 1
    response = requests.request("GET", f"{url}", headers=headers, data={})
    status_code = response.status_code
    if status_code != 200:
        logger.warning(f'Received irregular status code: {status_code}')
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

payload = {}

headers = {
  "Authorization": f"Bearer {ENV['ZOOM_TOKEN']}"
}

# https://marketplace.zoom.us/docs/api-reference/rate-limits
# 1 call a second
@sleep_and_retry
@limits(calls=60, period=60)
def run_report(param_attribute, headers, json_attribute_name, page_token):

    url = ENV[f'ZOOM_{param_attribute}_URL']
    params = ENV[f'ZOOM_{param_attribute}_PARAMS']

    # construct param string
    f = furl.furl('')
    f.args = params
    print(f.url)

    # get total page count
    total_page_count = get_total_page_count(f"{url}{f.url}", headers)

    logger.info(f"Total page number {total_page_count}")
    total_df = pd.DataFrame()

    # Either go by the page number or token
    while (page_token or params.get('page_number') <= total_page_count):
        if (params.get("page_number")):
            logger.info(f"Page Number: {params.get('page_number')} out of total page number {total_page_count}")
        # construct param string
        f = furl.furl('')
        f.args = params
        logger.info(f"Calling {url}{f.url}")

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
                if results.get("next_page_token"):
                    params["next_page_token"] = results["next_page_token"]
                elif params.get("page_number"):
                    params["page_number"] += 1
                else:
                    logger.info("No more tokens and not paged!")
                    break
            except JSONDecodeError:
                logger.exception('JSONDecodeError encountered')
                logger.info('No more pages!')

    # output csv file
    output_file_name = f"total_{json_attribute_name}.csv"
    total_df.to_csv(output_file_name)


# run users report
#run_report('USERS', headers, 'users', page_token=False)

# run meetings report
run_report('MEETINGS', headers, 'meetings', page_token=True)

# run webinars report
run_report('WEBINARS', headers, 'webinars', page_token=True)
