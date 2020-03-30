import requests
# standard libraries
import yaml
import json
import logging
import os

from typing import Dict, Union

# third-party libraries
import pandas as pd
import time
import dateparser
from datetime import datetime, timedelta

# Initialize settings and globals

logger = logging.getLogger(__name__)

# read configurations
try:
    with open(os.path.join('config', 'env.yaml')) as env_file:
        ENV = yaml.load(env_file.read())
except FileNotFoundError:
    logger.error(
        'Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

logger.info(ENV)

payload = {}

headers = {
  "Authorization": f"Bearer {ENV['ZOOM_TOKEN']}"
}

ZOOM_BASE_URL = ENV.get('ZOOM_BASE_URL', "")
DEFAULT_SLEEP_TIME = ENV.get('DEFAULT_SLEEP_TIME', "10")


# Functions
def get_total_page_count(url: str, headers: Dict[str, Union[str, int]] = {}, params: dict = {}):
    # get the total page count
    total_page_count = 0
    response = requests.request("GET", f"{url}", headers=headers, params=params)
    status_code = response.status_code
    if status_code != 200:
        logger.warning(f'Received irregular status code: {status_code}')
        logger.info('No page at all!')
    else:
        try:
            results = json.loads(response.text.encode('utf8'))
            total_page_count = results['page_count']
        except json.JSONDecodeError:
            logger.warning('JSONDecodeError encountered')
            logger.info('No page at all!')
    return total_page_count


def run_report(api_url: str, headers: dict, json_attribute_name: str,
               default_params: dict = {}, page_size: int = 300, page_token: bool = False, use_date: bool = False):
    url = ZOOM_BASE_URL+api_url
    params = default_params
    # If page size is specified use this
    if page_size:
        params['page_size'] = page_size
    total_list = []
    # TODO: Detect the date from the previous CSV
    # Either loop for all dates or just one a single report
    if use_date:
        early_date = dateparser.parse(ENV.get('ZOOM_EARLIEST_FROM', '2020-03-01')).date()
        # Only use the date as a paramter
        # Loop until yesterday (this will go until now() -1)
        for i in range((datetime.now().date() - early_date).days):
            param_date = early_date + timedelta(days=i)
            params["from"] = str(param_date)
            params["to"] = str(param_date)
            # Add this loop to the list
            total_list.extend(zoom_loop(url, headers, json_attribute_name, dict(params), page_token))
    else:
        total_list.extend(zoom_loop(url, headers, json_attribute_name, dict(params), page_token))
    # output csv file
    total_df = pd.DataFrame(total_list)
    total_df.index.name = "index_id"
    output_file_name = f"total_{json_attribute_name}.csv"
    # Remove any duplicate uuids in the record
    logger.info(f"Initial dataframe size: {len(total_df)}")
    if "uuid" in total_df:
        total_df.drop_duplicates("uuid", inplace=True)
        logger.info(f"Dataframe with duplicates removed: {len(total_df)}")

    # Sort columns alphabetically
    total_df.sort_index(axis=1, inplace=True)

    # Write to CSV
    total_df.to_csv(output_file_name)


def zoom_loop(url: str, headers: dict, json_attribute_name: str,
              params: dict, page_token: bool = False) -> list:
    # Need a fresh copy of dicts
    total_list = []    # get total page count
    total_page_count = get_total_page_count(url, headers, params)
    logger.info(f"Total page number {total_page_count}")
    # Either go by the page number or token
    while (page_token or params.get('page_number') <= total_page_count):
        if (params.get("page_number")):
            logger.info(f"Page Number: {params.get('page_number')} out of total page number {total_page_count}")

        response = requests.request(
            "GET", url, headers=headers, params=params)
        status_code = response.status_code
        # Rate limited, wait a few seconds
        if status_code == 429:
            # This is what the header should be
            retry_after = response.headers.get("Retry-After")
            sleep_time = DEFAULT_SLEEP_TIME
            if retry_after and retry_after.isdigit():
                logger.warning(f"Received status 429, need to wait for {retry_after}")
                sleep_time = retry_after
            else:
                logger.warning(f"No Retry-After header, setting sleep loop for {DEFAULT_SLEEP_TIME} seconds")
            while status_code == 429:
                time.sleep(sleep_time)
                response = requests.request(
                    "GET", url, headers=headers, params=params)
                status_code = response.status_code

        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            break
        else:
            try:
                results = json.loads(response.text.encode('utf8'))

                total_list.extend(results[json_attribute_name])
                logger.info(f'Current size of list: {len(total_list)}')

                # go retrieve next page
                if results.get("next_page_token"):
                    page_token = results.get("next_page_token")
                    params["next_page_token"] = page_token
                elif params.get("page_number"):
                    params["page_number"] += 1
                else:
                    logger.info("No more tokens and not paged!")
                    break
            except json.JSONDecodeError:
                logger.exception('YAMLError encountered')
                logger.info('No more pages!')
                break
    return total_list


# run users report
run_report('/v2/users', headers, 'users', {"status": "active", "page_number": 1}, page_token=False, use_date=False)
# run meetings report
run_report('/v2/metrics/meetings', headers, 'meetings', {"type": "past"}, page_token=True, use_date=True)
# run webinars report
run_report('/v2/metrics/webinars', headers, 'webinars', {"type": "past"}, page_token=True, use_date=True)
