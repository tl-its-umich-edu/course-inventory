# TODO: When fixing issue #56 remove this and fix the flake8 issues
# flake8: noqa
import glob
# standard libraries
import json
import logging
import os
import sys
import yaml
from typing import Dict, Sequence, Union

# local libraries
from db.db_creator import DBCreator
from vocab import ValidDataSourceName
from environ import ENV

from zipfile import ZipFile

import dateparser
# third-party libraries
import time
from datetime import datetime, timedelta
import pandas as pd
import requests

# Initialize settings and globals

logger = logging.getLogger(__name__)

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

# output settings
CREATE_CSVS = ENV.get('CREATE_CSVS', False)
INVENTORY_DB = ENV['INVENTORY_DB']
APPEND_TABLE_NAMES = ENV.get('APPEND_TABLE_NAMES', ['job_run', 'data_source_status'])

# template for BlueJeans API call
def bluejeans_api_call(method: str, url: str, headers_dict: Dict, payload_dict: Dict) -> requests.Response:
    # return when url is null
    if not url:
        logger.error("bluejeans_api_call url is null")
        return None
    
    try:
        response = requests.request(method, url, headers=headers_dict, data=json.dumps(payload_dict))
        if (response.status_code != 200):
            # status not of 200
            logger.error(f"bluejeans_api_call {url}: status {response.status_code}")
            return None

        # return response text
        return response
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        logger.error("bluejeans_api_call {url}: TimeOut exception")
    except requests.exceptions.TooManyRedirects:
        # URL was bad and try a different one
        logger.error("bluejeans_api_call {url}: TooManyRedirect exception")
    except requests.exceptions.RequestException:
        # other exceptions
        logger.error("bluejeans_api_call {url}: RequestException")

    return None

# get BlueJenas API token
def _1_get_bluejeans_api_token() -> str:
    token = None
    
    # prepare for the BlueJeans API call
    token_url = ENV.get('BLUEJEANS_URL_TOKEN')
    token_payload = {
        "grant_type": "client_credentials",
        "client_id": "Metrics_BJN",
        "client_secret": ENV.get('BLUEJEANS_CLIENT_SECRET')
    }
    token_headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    token_response = bluejeans_api_call("POST", token_url, token_headers, token_payload)
    if token_response:
        token_json = json.loads(token_response.text.encode('utf8'))
        token = token_json["access_token"]

        ## the expiration time in seconds
        expires_in_sec = token_json["expires_in"]
        ## minus for one minute, for safty check
        expires_in_sec = expires_in_sec - 60
        token_expiration_time = datetime.now() + timedelta(seconds=expires_in_sec)
        logger.info(f"token expires on: {token_expiration_time}")

    return token

# retrieve the download path for report zip file
def _2_get_file_download_path(headers_dict: Dict, payload_dict: Dict, job_id: str) -> str:
    path = ""
    # max 20 times
    max_count = 20
    count = 0
    while not path and count <= max_count:
        # retrieve report job information including the file download path
        jobs_url = ENV["BLUEJEANS_URL_USER_JOBS"]
        jobs_response = bluejeans_api_call("GET", jobs_url, headers_dict, payload_dict)
        if jobs_response:
            jobs_json = json.loads(jobs_response.text.encode('utf8'))
            for job_json in jobs_json:
                # find the right job:
                if job_json['_id'] == job_id:
                    if (job_json['status'] == 'success'):
                        download_file_path = job_json['message']['path']
                        logger.info(f"job {job_id} finished yet: {job_json['status']} and download file path is {download_file_path}")
                        return download_file_path
                    else:
                        logger.info(f"job {job_id} not finished yet: {job_json['status']}")
                        time.sleep(10)
        count += 1
    return path

# download report file and insert data into dataframe
def _3_download_report_file_read_into_dataframe(headers_dict: Dict,
                                            payload_dict: Dict,
                                            df_param: pd.DataFrame,
                                            download_file_path: str) -> pd.DataFrame:
    download_url = ENV["BLUEJEANS_URL_DOWNLOAD"]
    download_url = f"{download_url}{download_file_path}"

    # download the report file
    r = bluejeans_api_call("GET", download_url, headers_dict, payload_dict)

    # specifying the zip file name
    zip_file_name = os.path.join('data', ENV["BLUEJEANS_URL_DOWNLOAD_ZIP_NAME"])

    try:
        with open(zip_file_name, 'wb') as f:
            f.write(r.content)

        # opening the zip file in READ mode
        try:
            with ZipFile(zip_file_name, 'r') as zip_file:
                # printing all the contents of the zip file
                zip_file.printdir()
            
                # extracting all the files
                logger.info(f'Extracting all the files now:')
                zip_file.extractall()
                for csv_file in glob.glob(ENV["BLUEJEANS_URL_DOWNLOAD_ZIP_NAME_INNER_FILE"]):
                    logger.info(csv_file)
                    df = pd.read_csv(csv_file)

                    if df.size > 0:
                        # append to the total output
                        df_param = df_param.append(df)

                    # remove file
                    os.remove(csv_file)

            # remove zip file
            os.remove(zip_file_name)
            logger.info(f" {zip_file_name} file Removed!")
        except OSError:
            logger.error("OS error while opening downloaded zip file.")
        except ValueError:
            logger.error("ValueError while opening downloaded zip file.:")
        except:
            logger.error("Unexpected error while opening downloaded zip file.")
            raise
    except OSError:
        logger.error("OS error while downloading zip file.")
    except ValueError:
        logger.error("ValueError while downloading zip file.")
    except:
        logger.error("Unexpected error while downloading zip file.")
        raise

    return df_param

# dataframe def updates
def _4_clean_rename_columns (df: pd.DataFrame) -> pd.DataFrame: 
    # drop column start_time: the column is not well documented and contains misleading values
    logger.info(df)
    df = df.drop(columns=['start_time'])

    # rename dataframe column names, to "lower_case_with_underscores"
    # https://launchbylunch.com/posts/2014/Feb/16/sql-naming-conventions/#naming-conventions
    df = df.rename(columns={'meetingTitle': 'meeting_title',
                            'meetingId': 'meeting_id',
                            'userName': 'user_name',
                            'endTime': 'end_time',
                            'startTime': 'start_time',
                            'participantSeconds': 'participant_seconds',
                            'joinDate': 'join_date',
                            'joinWeek': 'join_week',
                            'joinMonth': 'join_month',
                            'participantMinutes': 'participant_minutes',
                            'meetingDurationMinutes': 'meeting_duration_minutes',
                            'popId': 'pop_id',
                            'userType': 'user_type',
                            'moderatorLess': 'moderator_less',
                            })
    return df

# the main function to call for BlueJeans report
def run_bluejeans_report() -> Sequence[Dict[str, Union[str, pd.Timestamp]]]:
    logger.info("run_bluejeans_report")

    # Insert gathered data
    # logger.info('prepare to insert BlueJeans data into DB')
    db_creator_obj = DBCreator(INVENTORY_DB, APPEND_TABLE_NAMES)

    total_df = pd.DataFrame()

    # 1: get the access token
    token = _1_get_bluejeans_api_token()
    if not token:
        logger.error("No API token. Exit for now. ")
        return
    
    # use token for request header
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    # get the start and end time for report
    # if the start and end dates are missing in ENV file, report for yesterday
    yesterday_date_string = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today_date_string = datetime.now().strftime('%Y-%m-%d')
    from_date = dateparser.parse(ENV.get('BLUEJEANS_FROM_DATE', yesterday_date_string)).date()
    to_date = dateparser.parse(ENV.get('BLUEJEANS_TO_DATE', today_date_string)).date()

    # query to see whether there is any existing meeting record within this date range
    from_date_string = from_date.strftime('%Y-%m-%d')
    to_date_string = to_date.strftime('%Y-%m-%d')
    meeting_results = db_creator_obj.engine.execute(f"""SELECT * FROM bluejeans_meeting where start_time > '{from_date_string}' and end_time < '{to_date_string}'""")
    meeting_results_size = len(meeting_results.fetchall())
    if meeting_results_size > 0:
        # meeting records exist for the given timeframe
        # stop and do not insert
        logger.warning(f"there are {meeting_results_size} meetings for the date interval {from_date_string} to {to_date_string}. Stop the script.")
        return []

    # Only use the date as a paramter
    # Loop until yesterday (this will go until now() -1)
    for i in range((to_date - from_date).days):
        param_date = from_date + timedelta(days=i)
        start_date = datetime(param_date.year, param_date.month, param_date.day, 0, 0, 0)
        end_date = datetime(param_date.year, param_date.month, param_date.day, 23, 59, 59)
        logger.info("start")
        logger.info(start_date.astimezone().isoformat())
        logger.info("end")
        logger.info(end_date.astimezone().isoformat())

        
        # post the report request job
        report_url = (
            ENV['BLUEJEANS_URL_REPORT'] +
            '?filter=[' +
            '{"type":"date","comparison":"lt","value":"' +
            end_date.astimezone().isoformat() +
            '","field":"end_time"},' +
            '{"type":"date","comparison":"lt","value":"' +
            end_date.astimezone().isoformat() +
            '","field":"start_time"},' +
            '{"type":"date","comparison":"gt","value":"' +
            start_date.astimezone().isoformat() +
            '","field":"start_time"}' +
            ']&fileName=meetings_date&userid=' +
            str(ENV.get('BLUEJEANS_USER_ID')) +
            '&app_name=command_center'
        )
        reportJobs_response = bluejeans_api_call("POST", report_url, headers, payload)
        logger.info(report_url)

        if reportJobs_response:
            reportJob_json = json.loads(reportJobs_response.text.encode('utf8'))
            if ("status" in reportJob_json) and (reportJob_json["status"] == "success") and ("jobId" in reportJob_json):
                # get the job id
                job_id = reportJob_json["jobId"]
                logger.info("report job id = " + job_id)

            # 2: get the url path for report file download
            download_file_path = _2_get_file_download_path(headers, payload, job_id)
            if download_file_path:
                #3: download the report file
                total_df = _3_download_report_file_read_into_dataframe(headers, payload, total_df, download_file_path)

    # 4: rename columns
    logger.info(total_df)
    total_df = _4_clean_rename_columns(total_df)

    # Remove any duplicate uuids in the record
    logger.info(f"Initial dataframe size: {len(total_df)}")
    if "meeting_uuid" in total_df:
        total_df.drop_duplicates("meeting_uuid", keep='first', inplace=True)
        logger.info(f"Dataframe with duplicates removed: {len(total_df)}")

    num_bluejeans_meeting_records = len(total_df)

    #if CREATE_CSVS:
    logger.info(f'Writing {num_bluejeans_meeting_records} bluejeans_meeting records to CSV')
    total_df.to_csv(os.path.join('data', 'bluejeans_meeting.csv'), index=False)
    logger.info('Wrote data to data/bluejeans_meeting.csv')

    # Record data source info for BlueJeans API
    bluejeans_data_source = {
        'data_source_name': ValidDataSourceName.BLUEJEANS_API,
        'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
    }

    logger.info(list(total_df.columns))
    logger.info(f'Inserting {num_bluejeans_meeting_records} bluejeans_meeting records to DB')
    total_df.to_sql('bluejeans_meeting', db_creator_obj.engine, if_exists='append', index=False)
    logger.info(f'Inserted data into bluejeans_meeting table in {db_creator_obj.db_name}')

    return [bluejeans_data_source]

# Main Program

if __name__ == "__main__":
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    run_bluejeans_report()