# TODO: When fixing issue #56 remove this and fix the flake8 issues
# flake8: noqa
import requests
# standard libraries
import json
import logging
import os
import sys
import yaml
from typing import Dict, Sequence, Union

# third-party libraries
import pandas as pd
import time
import dateparser
import pytz
from datetime import datetime, timedelta
import glob

from db.db_creator import DBCreator

# importing required modules
from zipfile import ZipFile

# Initialize settings and globals

logger = logging.getLogger(__name__)

# read configurations
try:
    with open(os.path.join(os.path.dirname(__file__), '../config/secrets/env.json')) as env_file:
        ENV = yaml.safe_load(env_file.read())
except FileNotFoundError:
    sys.exit(
        'Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

# output settings
CREATE_CSVS = ENV.get('CREATE_CSVS', False)
INVENTORY_DB = ENV['INVENTORY_DB']
logger.info(INVENTORY_DB)
APPEND_TABLE_NAMES = ENV.get('APPEND_TABLE_NAMES', ['job_run', 'data_source_status'])

def run_bluejeans_report() -> Sequence[Dict[str, Union[str, pd.Timestamp]]]:
    logger.info("run_bluejeans_report")

    # Record data source info for Canvas API
    bluejeans_data_source = {
        'data_source_name': 'BLUEJEANS_API',
        'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
    }

    url = "https://api.bluejeans.com/oauth2/token?Client"

    payload = "{ \"grant_type\": \"client_credentials\", \"client_id\": \"Metrics_BJN\", \"client_secret\": \"" + ENV.get('BLUEJEANS_CLIENT_SECRET') + "\"}"
    logger.info(payload)
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
    }

    token_expiration_time = datetime.now()
    response = requests.request("POST", url, headers=headers, data=payload).text.encode('utf8')
    token_json = json.loads(response)
    token = token_json["access_token"]
    logger.info(token)
    ## the expiration time in seconds
    expires_in_sec = token_json["expires_in"]
    ## minus for one minute, for safty check
    expires_in_sec = expires_in_sec - 60
    token_expiration_time = datetime.now() + timedelta(seconds=expires_in_sec)
    logger.info(f"{token} expires on: {token_expiration_time}")

    payload = {}
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    total_df = pd.DataFrame()

    # TODO: the earliest time is hardcoded now
    from_date = dateparser.parse(ENV.get('ZOOM_EARLIEST_FROM', '2020-03-31')).date()
    to_date = dateparser.parse(ENV.get('ZOOM_EARLIEST_TO', '2020-04-09')).date()

    # Only use the date as a paramter
    # Loop until yesterday (this will go until now() -1)
    for i in range((to_date - from_date).days):
        param_date = from_date + timedelta(days=i)
        start = datetime(param_date.year, param_date.month, param_date.day, 0, 0, 0)
        end = datetime(param_date.year, param_date.month, param_date.day, 23, 59, 59)
        logger.info("start")
        logger.info(start.astimezone().isoformat())
        logger.info("end")
        logger.info(end.astimezone().isoformat())
        
        # post the report request job
        url = "https://indigo-api.bluejeans.com/v1/enterprise/" + str(ENV.get('BLUEJEANS_ENTERPRISE_ID')) + "/indigo/reportJob/meetings?filter=\
        [{\"type\":\"date\",\"comparison\":\"lt\",\"value\":\"" + end.astimezone().isoformat() + "\",\"field\":\"end_time\"},\
        {\"type\":\"date\",\"comparison\":\"lt\",\"value\":\"" + end.astimezone().isoformat() + "\",\"field\":\"start_time\"},\
        {\"type\":\"date\",\"comparison\":\"gt\",\"value\":\"" + start.astimezone().isoformat() + "\",\"field\":\"start_time\"}]\
        &fileName=meetings_30th-Mar-2020_30th-Mar-2020&userid=" + str(ENV.get("BLUEJEANS_USER_ID")) + "&app_name=command_center"
        
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        
        reportJobs_response = requests.request("POST", url, headers=headers, data=payload)
        reportJob_json = json.loads(reportJobs_response.text.encode('utf8'))
        if ("status" in reportJob_json) and (reportJob_json["status"] == "success") and ("jobId" in reportJob_json):
            # get the job id
            job_id = reportJob_json["jobId"]
            logger.info("report job id = " + job_id)

        # sleep to wait for report job finish
        download_file_path = ""
        while not download_file_path :
            # retrieve report job information including the file download path
            jobs_url = f"https://indigo-api.bluejeans.com/v1/enterprise/{ENV.get('BLUEJEANS_ENTERPRISE_ID')}/indigo/jobs/list?userid={ENV.get('BLUEJEANS_USER_ID')}"
            payload = {}
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}'
            }
            jobs_response = requests.request("GET", jobs_url, headers=headers, data=payload)
            jobs_json = json.loads(jobs_response.text.encode('utf8'))
            for job_json in jobs_json:
                # find the right job:
                if job_json['_id'] == job_id:
                    if (job_json['status'] == 'success'):
                        download_file_path = job_json['message']['path']
                        logger.info(f"job {job_id} finished yet: {job_json['status']} and download file path is {download_file_path}")
                        break
                    else:
                        logger.info(f"job {job_id} not finished yet: {job_json['status']}")
                        time.sleep(10)

        download_url = f"https://indigo-api.bluejeans.com/v1/enterprise/{ENV.get('BLUEJEANS_ENTERPRISE_ID')}/indigo/{download_file_path}"

        # download the report file
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        r = requests.get(download_url, headers=headers)

        # specifying the zip file name
        zip_file_name = "./bj.zip"

        with open(zip_file_name, 'wb') as f:
            f.write(r.content)
        
        # opening the zip file in READ mode
        with ZipFile(zip_file_name, 'r') as zip_file:
            # printing all the contents of the zip file
            zip_file.printdir()
        
            # extracting all the files
            print('Extracting all the files now...')
            zip_file.extractall()
            for csv_file in glob.glob("./meetings_*.csv"):
                logger.info(csv_file)
                df = pd.read_csv(csv_file)

                # append to the total output
                total_df = total_df.append(df)
                logger.info(df)

                # remove file
                os.remove(csv_file)

        # remove zip file
        os.remove(zip_file_name)
        print(f" {zip_file_name} file Removed!")

    # Remove any duplicate uuids in the record
    logger.info(f"Initial dataframe size: {len(total_df)}")
    if "meeting_uuid" in total_df:
        total_df.drop_duplicates("meeting_uuid", keep='first', inplace=True)
        logger.info(f"Dataframe with duplicates removed: {len(total_df)}")

    if CREATE_CSVS:
        total_df.to_csv("./bluejeans_meetings.csv")

    # Empty tables (if any) in database, then migrate
    logger.info('Emptying tables in DB')
    db_creator_obj = DBCreator(INVENTORY_DB, APPEND_TABLE_NAMES)

    # Insert gathered data
    logger.info(list(total_df.columns) )
    num_bluejeans_meeting_records = len(total_df)
    logger.info(f'Inserting {num_bluejeans_meeting_records} bluejeans meetings records to DB')
    total_df.to_sql('bluejeans_meetings', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into bluejeans_meetings table in {db_creator_obj.db_name}')

    return [bluejeans_data_source]

# Main Program

if __name__ == "__main__":
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    run_bluejeans_report()