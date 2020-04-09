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
import jwt

# Initialize settings and globals

logger = logging.getLogger(__name__)

# read configurations
try:
    with open(os.path.join(os.path.dirname(__file__), '../config/secrets/env.json')) as env_file:
        ENV = yaml.safe_load(env_file.read())
except FileNotFoundError:
    logger.error(
        'Configuration file could not be found; please add env.yaml to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

logger.info(ENV)

DEFAULT_SLEEP_TIME = ENV.get('DEFAULT_SLEEP_TIME', 10)


def get_request_retry(url: str, headers: Dict[str, Union[str, int]],
                      params: Dict[str, Union[str, int]]) -> requests.Response:

    response = requests.request("GET", url, headers=headers, params=params)

    # Rate limited, wait a few seconds
    if response.status_code == requests.codes.too_many_requests:
        # This is what the header should be
        retry_after = response.headers.get("Retry-After")
        sleep_time = DEFAULT_SLEEP_TIME
        if retry_after and retry_after.isdigit():
            logger.warning(
                f"Received status 429, need to wait for {retry_after}")
            sleep_time = retry_after
        else:
            logger.warning(
                f"No Retry-After header, setting sleep loop for {DEFAULT_SLEEP_TIME} seconds")
        while response.status_code == requests.codes.too_many_requests:
            time.sleep(sleep_time)
            response = requests.request(
                "GET", url, headers=headers, params=params)

    # If it's not okay at this point, raise an error
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    return response


# Functions
def get_total_page_count(url: str, headers: Dict[str, Union[str, int]], params: Dict[str, Union[str, int]]):
    # get the total page count
    total_page_count = 0
    try:
        response = get_request_retry(url, headers, params)
    except requests.exceptions.HTTPError:
        logger.exception('Received irregular status code during request')
        return 0
    try:
        results = json.loads(response.text.encode('utf8'))
        total_page_count = results['page_count']
    except json.JSONDecodeError:
        logger.warning('JSONDecodeError encountered')
        logger.info('No page at all!')
    return total_page_count


def run_report(api_url: str, json_attribute_name: str,
               default_params: Dict[str, Union[str, int]] = None, page_size: int = 300,
               page_token: bool = False, use_date: bool = False):
    if default_params is None:
        default_params = {}
    params = default_params
    # If page size is specified use this
    if page_size:
        params['page_size'] = page_size
    total_list = []
    # TODO: Detect the date from the previous CSV
    # Either loop for all dates or just one a single report

    for zoom_key, zoom_config in ENV["ZOOM_CONFIG"].items():
        logger.info(f"Starting zoom pull for instance {zoom_key}")
        url = zoom_config["BASE_URL"] + api_url
        token = jwt.encode(
            {'iss': zoom_config["API_KEY"],
             'exp': datetime.utcnow() + timedelta(minutes=30)
             }, zoom_config["API_SECRET"], algorithm='HS256')
        headers = {
            # Need to decode https://github.com/jpadilla/pyjwt/issues/391
            "Authorization": f"Bearer {token.decode('utf-8')}"
        }
        if use_date and "EARLIEST_FROM" in zoom_config:
            early_date = dateparser.parse(zoom_config["EARLIEST_FROM"]).date()
            # Only use the date as a parameter
            # Loop until yesterday (this will go until now() -1)
            for i in range((datetime.now().date() - early_date).days):
                param_date = early_date + timedelta(days=i)
                params["from"] = str(param_date)
                params["to"] = str(param_date)
                logger.info(f"Pulling data from date {param_date}")
                # Add this loop to the list
                zoom_list = zoom_loop(url, headers, json_attribute_name, dict(params), page_token)
        else:
            zoom_list = zoom_loop(url, headers, json_attribute_name, dict(params), page_token)
        # Add the instance this was pulled from to each of the results
        for list_item in zoom_list:
            list_item.update({"media_instance": zoom_key})
        total_list.extend(zoom_list)
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


def zoom_loop(url: str, headers: Dict[str, Union[str, int]], json_attribute_name: str,
              params: Dict[str, Union[str, int]], page_token: bool = False) -> list:
    # Need a fresh copy of dicts
    total_list = []    # get total page count
    total_page_count = get_total_page_count(url, headers, params)
    logger.info(f"Total page number {total_page_count}")
    # Either go by the page number or token
    while (page_token or params.get('page_number') <= total_page_count):
        if (params.get("page_number")):
            logger.info(f"Page Number: {params.get('page_number')} out of total page number {total_page_count}")
        try:
            response = get_request_retry(url, headers=headers, params=params)
        except requests.exceptions.HTTPError:
            logger.exception('Received irregular status code during request')
            break
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
            logger.exception('JSONDecodeError encountered')
            logger.info('No more pages!')
            break
    return total_list


# run users report
run_report('/v2/users', 'users', {"status": "active", "page_number": 1}, page_token=False, use_date=False)
# run meetings report
run_report('/v2/metrics/meetings', 'meetings', {"type": "past"}, page_token=True, use_date=True)
# run webinars report
run_report('/v2/metrics/webinars', 'webinars', {"type": "past"}, page_token=True, use_date=True)
