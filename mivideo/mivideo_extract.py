# -*- coding: utf-8 -*-
import logging
import os
import time
from typing import Union, Sequence, Dict

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy.engine import ResultProxy

import mivideo.queries as queries
from db.db_creator import DBCreator
from environ import ENV, CONFIG_DIR
from vocab import ValidDataSourceName

logger = logging.getLogger(__name__)

SHAPE_ROWS: int = 0  # Index of row count in DataFrame.shape() array


class MiVideoExtract(object):
    udpKeyFilePath: str
    credentials: service_account.Credentials
    udpDb: bigquery.Client
    appDb: DBCreator

    def __init__(self):
        self.udpKeyFilePath = os.path.join(CONFIG_DIR, ENV.get('mivideo', {}).get(
            'service_account_json_filename'))
        logger.debug(f'udpKeyFilePath: "{self.udpKeyFilePath}"')

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
        self.appDb.set_up()

    def _readTableLastTime(self, tableName: str, tableColumnName: str) -> Union[str, None]:
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

        tableName = 'mivideo_media_started_hourly'

        logger.debug(f'"{tableName}" - Starting procedure...')

        lastTime: str = self._readTableLastTime(tableName, 'event_time_utc_latest')

        if (lastTime):
            logger.debug(f'"{tableName}" - Last time found in table: "{lastTime}"')
        else:
            lastTime = '2020-03-01'
            logger.debug(
                f'"{tableName}" - Last time not found in table; using default time: "{lastTime}"')

        logger.debug(f'"{tableName}" - Running query...')

        dfCourseEvents: pd.DataFrame = self.udpDb.query(
            queries.dfCourseEventsQuery(lastTime)).to_dataframe()

        logger.debug(f'"{tableName}" - Completed query.')

        if (not dfCourseEvents.empty):
            logger.debug(
                f'"{tableName}" - Number of rows returned: ({dfCourseEvents.shape[SHAPE_ROWS]})')

            logger.debug(f'"{tableName}" - Saving to table...')

            dfCourseEvents.to_sql(tableName, self.appDb.engine, if_exists='append', index=False)

            logger.debug(f'"{tableName}" - Saved.')
        else:
            logger.debug(f'"{tableName}" - No rows returned.')

        logger.debug(f'"{tableName}" - Procedure complete.')

        logger.debug('End of extract')

        return [{
            'data_source_name': ValidDataSourceName.UNIZIN_DATA_PLATFORM,
            'data_updated_at': pd.to_datetime(time.time(), utc=True)
        }]


def main():
    return MiVideoExtract().run()


if '__main__' == __name__:
    main()
