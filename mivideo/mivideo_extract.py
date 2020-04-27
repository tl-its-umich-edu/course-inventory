# -*- coding: utf-8 -*-
'''
Module for setting up and running the MiVideo data extract.
'''

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Sequence, Union

import pandas as pd
import pytz
from KalturaClient import *
from KalturaClient.Plugins.Core import *
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy.engine import ResultProxy

import mivideo.queries as queries
from db.db_creator import DBCreator
from environ import CONFIG_DIR, CONFIG_PATH, ENV
from vocab import ValidDataSourceName

logger = logging.getLogger(__name__)

SHAPE_ROWS: int = 0  # Index of row count in DataFrame.shape() array


class MiVideoExtract(object):
    '''
    Initialize the MiVideo data extract process by instantiating the ``MiVideoExtract`` class,
    then invoke its ``run()`` method.

    For example, ``MiVideoExtract().run()``.
    '''

    def __init__(self):
        udpKeyFileName: str = ENV.get('MIVIDEO', {}).get('service_account_json_filename')
        if (udpKeyFileName is None):
            errorMessage: str = (f'"MIVIDEO.service_account_json_filename" '
                                 f'was not found in {CONFIG_PATH}')
            logger.error(errorMessage)
            raise ValueError(errorMessage)

        self.udpKeyFilePath: str = os.path.join(CONFIG_DIR, udpKeyFileName)
        logger.debug(f'udpKeyFilePath: "{self.udpKeyFilePath}"')

        self.credentials: service_account.Credentials = (
            service_account.Credentials.from_service_account_file(
                self.udpKeyFilePath,
                scopes=['https://www.googleapis.com/auth/cloud-platform'])
        )

        self.udpDb: bigquery.Client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )

        logger.info(f'Connected to BigQuery project: "{self.udpDb.project}"')

        dbParams: Dict = ENV['INVENTORY_DB']
        appendTableNames: Sequence[str] = ENV.get('APPEND_TABLE_NAMES', [
            'job_run', 'data_source_status', 'mivideo_media_started_hourly',
            'mivideo_media_created'])

        self.appDb: DBCreator = DBCreator(dbParams, appendTableNames)
        self.appDb.set_up()

    def _readTableLastTime(self, tableName: str, tableColumnName: str) -> Union[datetime, None]:
        lastTime: Union[datetime, None]

        try:
            sql: str = f'select max(t.{tableColumnName}) from {tableName} t'
            result: ResultProxy = self.appDb.engine.execute(sql)
            lastTime = result.fetchone()[0]
        except(Exception):
            lastTime = None

        return lastTime

    def mediaStartedHourly(self) -> Dict[str, Union[ValidDataSourceName, pd.Timestamp]]:
        """
        :return: Sequence of result dictionaries, with ValidDataSourceName and last run timestamp
        """

        tableName: str = 'mivideo_media_started_hourly'

        logger.info(f'"{tableName}" - Starting procedure...')

        lastTime: Union[datetime, None] = self._readTableLastTime(tableName,
                                                                  'event_time_utc_latest')

        if (lastTime):
            logger.info(f'"{tableName}" - Last time found in table: "{lastTime}"')
        else:
            lastTime = datetime(2020, 3, 1, tzinfo=timezone.utc)  # 2020-03-01
            logger.info(
                f'"{tableName}" - Last time not found in table; using default time: "{lastTime}"')

        logger.debug(f'"{tableName}" - Running query...')

        dfCourseEvents: pd.DataFrame = self.udpDb.query(
            queries.COURSE_EVENTS, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('startTime', 'DATETIME', lastTime),
                ]
            )
        ).to_dataframe()

        logger.debug(f'"{tableName}" - Completed query.')

        if (not dfCourseEvents.empty):
            logger.info(
                f'"{tableName}" - Number of rows returned: ({dfCourseEvents.shape[SHAPE_ROWS]})')

            logger.debug(f'"{tableName}" - Saving to table...')

            dfCourseEvents.to_sql(tableName, self.appDb.engine, if_exists='append', index=False)

            logger.debug(f'"{tableName}" - Saved.')
        else:
            logger.info(f'"{tableName}" - No rows returned.')

        logger.info(f'"{tableName}" - Procedure complete.')

        logger.info('End of extract')

        return {
            'data_source_name': ValidDataSourceName.UNIZIN_DATA_PLATFORM_EVENTS,
            'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
        }

    def mediaCreation(self) -> Dict[str, Union[ValidDataSourceName, pd.Timestamp]]:
        KALTURA_MAX_MATCHES_ERROR: str = 'QUERY_EXCEEDED_MAX_MATCHES_ALLOWED'
        # TIMESTAMP_TIME_ZONE: str = 'America/Detroit'
        TIMESTAMP_TIME_ZONE: str = 'UTC'

        kPartnerId: int = 1038472  # U of Michigan (KMC1)
        kUserId: str = 'mivideot3@umich.edu'
        kUserSecret: str = '95a4c677d53155fcb4ac441bd69bc0e9'
        kUserType: int = KalturaSessionType.ADMIN

        config = KalturaConfiguration(kPartnerId)
        config.serviceUrl = 'https://www.kaltura.com/'
        kClient: KalturaRequestConfiguration = KalturaClient(config)

        kSessionKey = kClient.session.start(kUserSecret, kUserId, kUserType, kPartnerId)
        kClient.setKs(kSessionKey)

        procedureName: str = 'mediaCreation'

        lastTime: Union[datetime, None] = self._readTableLastTime('mivideo_media_created',
                                                                  'created_at')

        if (lastTime):
            logger.info(f'"{procedureName}" - Last time found in table: "{lastTime}"')
        else:
            lastTime = datetime(2020, 3, 1, tzinfo=timezone.utc)  # 2020-03-01
            logger.info(
                f'"{procedureName}" - Last time not found in table; using default time: "{lastTime}"')

        createdAtTimestamp: float = datetime.fromisoformat('2020-03-20 00:00:00-04:00').timestamp()

        kFilter = KalturaMediaEntryFilter()
        kFilter.createdAtGreaterThanOrEqual = createdAtTimestamp
        kFilter.categoriesFullNameIn = 'Canvas_UMich'
        kFilter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC

        kPager = KalturaFilterPager()
        kPager.pageSize = 500  # 500 is maximum
        kPager.pageIndex = 1

        mediaCount: int = 0
        results: Sequence[KalturaMediaEntry] = None
        lastCreatedAtTimestamp: Union[float, int] = createdAtTimestamp

        while True:
            try:
                results = kClient.media.list(kFilter, kPager).objects
            except Exception as kException:
                if (KALTURA_MAX_MATCHES_ERROR in kException.args):
                    # set new filter timestamp and reset pager, then continue
                    # add one second to avoid dupes, but could it skip media with similar timestamp?
                    # maybe use DB functions to silently skip dupes instead
                    kFilter.createdAtGreaterThanOrEqual = lastCreatedAtTimestamp + 1
                    kPager.pageIndex = 1
                    continue
                else:
                    logger.debug(f'Other Kaltura API error: "{kException}"')
                    break

            for media in results:
                mediaCount += 1
                logger.debug([
                    media.id,
                    datetime.fromtimestamp(media.createdAt,
                                           pytz.timezone(TIMESTAMP_TIME_ZONE)).isoformat(),
                    media.name, media.duration, media.categories])

            lastCreatedAtTimestamp = results[-1].createdAt

            if (len(results) < kPager.pageSize):
                break

            kPager.pageIndex += 1

        return {
            'data_source_name': ValidDataSourceName.KALTURA_API,
            'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
        }

    def run(self) -> Sequence[Dict[str, Union[ValidDataSourceName, pd.Timestamp]]]:
        return [
            # self.mediaStartedHourly(),
            self.mediaCreation(),
        ]


def main() -> Sequence[Dict[str, Union[ValidDataSourceName, pd.Timestamp]]]:
    '''
    This method is invoked when its module is executed as a standalone program.

    :return: Sequence of result dictionaries, with ValidDataSourceName and last run timestamp
    '''
    return MiVideoExtract().run()


if '__main__' == __name__:
    main()
