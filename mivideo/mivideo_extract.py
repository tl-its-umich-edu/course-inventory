# -*- coding: utf-8 -*-
import csv
import logging
import os
import sys
from io import StringIO
from os import path
from typing import Union, Sequence, Dict

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy.engine import ResultProxy

import mivideo.queries as queries
from db.db_creator import DBCreator
from environ import ENV, CONFIG_PATH, CONFIG_DIR

logger = logging.getLogger(__name__)

CSV_OPTIONS: dict = {
    'index': False,
    'quoting': csv.QUOTE_NONNUMERIC,
    'mode': 'a',  # append to end of file
}

SHAPE_COLUMNS: int = 0  # Index of column count in DataFrame.shape array

lastTimeOverride: str = sys.argv[1] if (len(sys.argv) > 1) else None


class MiVideoExtract(object):
    udpKeyFilePath: str
    credentials: service_account.Credentials
    udpDb: bigquery.Client
    appDb: DBCreator

    def __init__(self):
        self.udpKeyFilePath = os.path.join(CONFIG_DIR, ENV.get('mivideo', {}).get(
            'service_account_json_filename'))

        self.credentials = service_account.Credentials.from_service_account_file(
            self.udpKeyFilePath,
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
        )

        self.udpDb = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

        logger.debug(f'Connected to BigQuery project: "{self.udpDb.project}"')

        DB_PARAMS = ENV['INVENTORY_DB']
        APPEND_TABLE_NAMES = ENV.get('APPEND_TABLE_NAMES', ['job_run'])

        self.appDb = DBCreator(DB_PARAMS, APPEND_TABLE_NAMES)
        logger.debug(f'conn_str: {self.appDb.conn_str}')

        self.appDb.set_up()
        logger.debug(f'conn: {self.appDb.conn}')
        logger.debug(f'appDb table names: {self.appDb.get_table_names()}')

    def readFileLastLine(self, filename: str) -> Union[str, None]:
        lastLine: str = None

        try:
            with open(filename, 'r') as f:
                lastLine = f.readlines()[-1]
        except:
            pass

        return lastLine

    def readFileLastTime(self, filename: str, timeColumnNumber: int) -> Union[str, None]:
        lastTime: str = None

        try:
            lastTime = (
                pd.read_csv(StringIO(self.readFileLastLine(filename)), header=None)
                    .iloc[0][timeColumnNumber]
            )
        except:
            pass

        return lastTime

    def readTableLastTime(self, tableName: str, tableColumnName: str) -> Union[str, None]:
        lastTime: str = None

        try:
            sql: str = f'select max(t.{tableColumnName}) from {tableName} t'
            result: ResultProxy = self.appDb.engine.execute(sql)
            lastTime = result.fetchone()[0]
        except:
            pass

        return lastTime

    def run(self) -> Sequence[Dict[str, Union[str, pd.Timestamp]]]:
        """

        :return: Sequence of result dictionaries, with string keys and last run timestamp
        """

        # udpKeyFilePath: str = os.path.join(CONFIG_DIR,
        #                              ENV.get('mivideo', {}).get('service_account_json_filename'))

        logger.debug(f'CONFIG_PATH: {CONFIG_PATH}')
        logger.debug(f'CONFIG_DIR: {CONFIG_DIR}')
        logger.debug(f'udpKeyFilePath: {self.udpKeyFilePath}')

        tableName = 'mivideo_media_started_hourly'

        logger.debug(f'"{tableName}" - Starting procedure…')

        lastTime = self.readTableLastTime(tableName, 'event_time_utc_latest') or '2020-03-01'

        logger.debug(f'"{tableName}" - Last time found in file: "{lastTime}"')

        logger.debug(f'"{tableName}" - Running query…')

        dfCourseEvents: pd.DataFrame = self.udpDb.query(
            queries.dfCourseEventsQuery(lastTime)).to_dataframe()

        logger.debug(f'"{tableName}" - Completed query.')

        if (not dfCourseEvents.empty):
            logger.debug(
                f'"{tableName}" - Number of rows returned: ({dfCourseEvents.shape[SHAPE_COLUMNS]})')

            # includeHeaders = not path.exists(tableName)

            # logger.debug(
            #     f'"{tableName}" - Saving to file with{"" if includeHeaders else "out"} headers…')
            logger.debug(
                f'"{tableName}" - Saving to table…')

            # dfCourseEvents.to_csv(
            #     tableName,
            #     header=includeHeaders,
            #     **CSV_OPTIONS)

            dfCourseEvents.to_sql(tableName, self.appDb.engine, if_exists='append', index=False)

            logger.debug(f'"{tableName}" - Saved.')
        else:
            logger.debug(f'"{tableName}" - No rows returned.')

        logger.debug(f'"{tableName}" - Procedure complete.')

        return []  # 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑

        sourceFilename = tableName
        tableName = 'mivideo_media_started_hourly_all.csv'

        logger.debug(f'"{tableName}" - Starting procedure…')

        if (not dfCourseEvents.empty):
            logger.debug(f'"{tableName}" - Calculating new data from "{sourceFilename}"…')

            dfAllEvents: pd.DataFrame = (
                (dfCourseEvents.drop(columns='course_id').groupby(['event_hour_utc']).sum())
                    .sort_values(by='event_hour_utc')
                    .reset_index())

            logger.debug(f'"{tableName}" - Data calculated.')

            logger.debug(
                f'"{tableName}" - Number of rows calculated: ({dfAllEvents.shape[SHAPE_COLUMNS]})')

            includeHeaders = not path.exists(tableName)

            logger.debug(
                f'"{tableName}" - Saving to file with{"" if includeHeaders else "out"} headers…')

            dfAllEvents.to_csv(
                tableName,
                header=includeHeaders,
                **CSV_OPTIONS)
        else:
            logger.debug(f'"{tableName}" - Not updating.  No new data from "{sourceFilename}".')

        logger.debug(f'"{tableName}" - Starting procedure…')
        logger.debug(f'"{tableName}" - Procedure complete.')

        tableName = 'mivideo_media_creation.csv'

        logger.debug(f'"{tableName}" - Starting procedure…')

        lastTime = lastTimeOverride or self.readFileLastTime(tableName,
                                                             timeColumnNumber=5) or '1970-01-01'

        logger.debug(f'"{tableName}" - Last time found in file: "{lastTime}"')

        logger.debug(f'"{tableName}" - Running query…')

        dfMivideoCreation: pd.DataFrame = client.query(
            queries.dfMivideoCreationQuery(lastTime)).to_dataframe()

        logger.debug(f'"{tableName}" - Completed query.')

        if (not dfMivideoCreation.empty):
            logger.debug(
                f'"{tableName}" - Number of rows returned: ({dfMivideoCreation.shape[SHAPE_COLUMNS]})')

            includeHeaders = not path.exists(tableName)

            logger.debug(
                f'"{tableName}" - Saving to file with{"" if includeHeaders else "out"} headers…')

            dfMivideoCreation.to_csv('mivideo_media_creation.csv', **CSV_OPTIONS)
        else:
            logger.debug(f'"{tableName}" - No rows returned.')

        logger.debug(f'"{tableName}" - Procedure complete.')

        logger.debug('🏁 End of Report')


def main():
    return MiVideoExtract().run()


if '__main__' == __name__:
    main()
