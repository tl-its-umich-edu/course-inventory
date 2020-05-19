# -*- coding: utf-8 -*-
'''
Module for setting up and running the MiVideo data extract.
'''
import logging
import os
from datetime import datetime
from typing import Dict, Iterable, Sequence, Union

import pandas as pd
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaFilterPager, KalturaMediaEntry,
    KalturaMediaEntryFilter, KalturaMediaEntryOrderBy, KalturaMediaService,
    KalturaRequestConfiguration, KalturaSessionService, KalturaSessionType
)
from KalturaClient.exceptions import KalturaException
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.io.sql import SQLTable
from sqlalchemy.engine import Connection, ResultProxy
from sqlalchemy.exc import SQLAlchemyError

import mivideo.queries as queries
from db.db_creator import DBCreator
from environ import CONFIG_DIR, ENV
from vocab import DataSourceStatus, ValidDataSourceName

logger = logging.getLogger(__name__)

SHAPE_ROWS: int = 0  # Index of row count in DataFrame.shape() array


class MiVideoExtract:
    '''
    Initialize the MiVideo data extract process by instantiating the ``MiVideoExtract`` class,
    then invoke its ``run()`` method.

    For example, ``MiVideoExtract().run()``.
    '''

    def __init__(self):
        self.mivideoConfig: Dict = ENV['MIVIDEO']
        self.defaultLastTimestamp: str = self.mivideoConfig.get(
            'default_last_timestamp', '2020-03-01T00:00:00+00:00'
        )

        dbParams: Dict = ENV['INVENTORY_DB']
        self.appDb: DBCreator = DBCreator(dbParams)

        self.kPartnerId: int
        self.kUserId: str
        self.kUserSecret: str
        self.categoriesFullNameIn: str

    def _udpConnect(self) -> bigquery.Client:
        udpKeyFileName: str = self.mivideoConfig['udp_service_account_json_filename']

        udpKeyFilePath: str = os.path.join(CONFIG_DIR, udpKeyFileName)
        logger.debug(f'udpKeyFilePath: "{udpKeyFilePath}"')

        udpCredentials: service_account.Credentials = (
            service_account.Credentials.from_service_account_file(
                udpKeyFilePath,
                scopes=['https://www.googleapis.com/auth/cloud-platform'])
        )

        udpDb: bigquery.Client = bigquery.Client(
            credentials=udpCredentials,
            project=udpCredentials.project_id
        )

        logger.info(f'Connected to BigQuery project: "{udpDb.project}"')

        return udpDb

    def _kalturaInit(self):
        self.kPartnerId = self.mivideoConfig['kaltura_partner_id']
        self.kUserSecret = self.mivideoConfig['kaltura_user_secret']
        self.categoriesFullNameIn = self.mivideoConfig.get(
            'kaltura_categories_full_name_in', 'Canvas_UMich')

    def _readTableLastTime(
            self,
            tableName: str,
            tableColumnName: str,
            defaultTime: Union[str, None] = None
    ) -> datetime:
        '''
        :param tableName: Name of table to search for timestamp.
        :param tableColumnName: Column of table to contain timestamp.
        :param defaultTime: Default timestamp to use if not found in table.
        :raises ValueError: When `defaultTime` is needed, but is set to `None`.
        :return:
        '''
        lastTime: Union[datetime, None]

        if (defaultTime is None):
            logger.warning('received defaultTime argument of (None)')

        try:
            sql: str = f'select max(t.{tableColumnName}) from {tableName} t'
            result: ResultProxy = self.appDb.engine.execute(sql)
            lastTime = result.fetchone()[0]
            if (lastTime):
                logger.info(f'Last time found in table "{tableName}": "{lastTime.isoformat()}"')
        except SQLAlchemyError:
            logger.info(f'Error getting max "{tableColumnName}" from "{tableName}"')
            lastTime = None

        if (lastTime is None):
            if (defaultTime is not None):
                lastTime = datetime.fromisoformat(self.defaultLastTimestamp)
                logger.info(
                    f'Last time not found in table "{tableName}"; '
                    f'returning default time, "{lastTime.isoformat()}"')
            else:
                raise ValueError('Unable to use defaultTime value of (None)')

        return lastTime

    def mediaStartedHourly(self) -> DataSourceStatus:
        """
        Update data from Kaltura Caliper events stored in UDP.

        :return: DataSourceStatus
        """

        udpDb: bigquery.Client = self._udpConnect()

        tableName: str = 'mivideo_media_started_hourly'

        localLogger = logging.getLogger(f'{logger.name}.mediaStartedHourly')

        localLogger.info('Starting procedure...')

        lastTime: datetime = self._readTableLastTime(
            tableName, 'event_time_utc_latest', self.defaultLastTimestamp
        )

        localLogger.debug('Running query...')

        dfCourseEvents: pd.DataFrame = udpDb.query(
            queries.COURSE_EVENTS, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('startTime', 'DATETIME', lastTime),
                ]
            )
        ).to_dataframe()

        localLogger.debug('Completed query.')

        if (not dfCourseEvents.empty):
            localLogger.info(
                f'Number of rows returned: ({dfCourseEvents.shape[SHAPE_ROWS]})')

            localLogger.debug('Saving to table...')

            dfCourseEvents.to_sql(tableName, self.appDb.engine, if_exists='append', index=False)

            localLogger.debug('Saved.')
        else:
            localLogger.info('No rows returned.')

        localLogger.info('Procedure complete.')

        return DataSourceStatus(ValidDataSourceName.UNIZIN_DATA_PLATFORM_EVENTS)

    @staticmethod
    def _queryRunner(
            pandasTable: SQLTable,
            dbConn: Connection,
            columnNameList: Sequence[str],
            data: Iterable):
        '''
        This handles the fairly rare occurrence of conflicting keys when
        inserting data into a table.
        '''

        tableFullName = pandasTable.name
        if (pandasTable.schema):
            tableFullName = f'{pandasTable.schema}.{tableFullName}'
        columnNames = ', '.join(columnNameList)
        valuePlaceholders = ', '.join(['%s'] * len(columnNameList))

        sql = (
            f'INSERT INTO {tableFullName} ({columnNames}) VALUES ({valuePlaceholders}) '
            f'ON DUPLICATE KEY UPDATE {columnNameList[0]}={columnNameList[0]}'  # magic
        )

        dbConn.execute(sql, list(data))

    def mediaCreation(self) -> DataSourceStatus:
        """
        Update data with Kaltura media metadata from Kaltura API.

        :return: DataSourceStatus
        """

        self._kalturaInit()

        KALTURA_MAX_MATCHES_ERROR: str = 'QUERY_EXCEEDED_MAX_MATCHES_ALLOWED'
        tableName: str = 'mivideo_media_created'

        localLogger = logging.getLogger(f'{logger.name}.mediaCreation')
        localLogger.info('Starting procedure...')

        kClient: KalturaRequestConfiguration = KalturaClient(KalturaConfiguration())
        kClient.setKs(  # pylint: disable=no-member
            KalturaSessionService(kClient).start(
                self.kUserSecret, type=KalturaSessionType.ADMIN, partnerId=self.kPartnerId))
        kMedia = KalturaMediaService(kClient)

        lastTime: datetime = self._readTableLastTime(
            tableName, 'created_at', self.defaultLastTimestamp)

        createdAtTimestamp: float = lastTime.timestamp()

        kFilter = KalturaMediaEntryFilter()
        kFilter.createdAtGreaterThanOrEqual = createdAtTimestamp
        kFilter.categoriesFullNameIn = self.categoriesFullNameIn
        kFilter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC

        kPager = KalturaFilterPager()
        kPager.pageSize = 500  # 500 is maximum
        kPager.pageIndex = 1

        results: Sequence[KalturaMediaEntry] = None
        lastCreatedAtTimestamp: Union[float, int] = createdAtTimestamp
        lastId: Union[str, None] = None
        numberResults: int = 0
        queryPageNumber: int = kPager.pageIndex  # for logging purposes
        totalNumberResults: int = numberResults  # for logging purposes
        endOfResults = False

        while not endOfResults:
            try:
                results = kMedia.list(kFilter, kPager).objects
            except KalturaException as kException:
                if (KALTURA_MAX_MATCHES_ERROR in kException.args):
                    # set new filter timestamp, reset pager to page 1, then continue
                    kFilter.createdAtGreaterThanOrEqual = lastCreatedAtTimestamp
                    localLogger.debug(
                        f'New filter timestamp: ({kFilter.createdAtGreaterThanOrEqual})')

                    # to avoid dupes, also filter out the last ID returned by previous query
                    # because Kaltura compares createdAt greater than *or equal* to timestamp
                    kFilter.idNotIn = lastId
                    kPager.pageIndex = 1
                    continue

                localLogger.info(f'Other Kaltura API error: "{kException}"')
                break

            numberResults = len(results)
            localLogger.debug(
                f'Query page ({queryPageNumber}); number of results: ({numberResults})')

            if (numberResults > 0):
                resultDictionaries: Sequence[Dict] = tuple(r.__dict__ for r in results)

                creationData: pd.DataFrame = self._makeCreationData(resultDictionaries)

                creationData.to_sql(
                    tableName, self.appDb.engine, if_exists='append', index=False)

                courseData: pd.DataFrame = self._makeCourseData(
                    resultDictionaries, kFilter.categoriesFullNameIn)

                courseData.to_sql('mivideo_media_courses', self.appDb.engine, if_exists='append',
                                  index=False, method=self._queryRunner)

                lastCreatedAtTimestamp = results[-1].createdAt
                lastId = results[-1].id
                totalNumberResults += numberResults

            endOfResults = (numberResults < kPager.pageSize)

            kPager.pageIndex += 1
            queryPageNumber += 1

        localLogger.info(f'Total number of results: ({totalNumberResults})')

        localLogger.info('Procedure complete.')

        return DataSourceStatus(ValidDataSourceName.KALTURA_API)

    @staticmethod
    def _makeCourseData(resultDictionaries: Sequence[Dict], categoryFilter: str) -> pd.DataFrame:
        courseData: pd.DataFrame = pd.DataFrame.from_records(
            resultDictionaries, columns=('id', 'categories',)
        ).rename(columns={'id': 'media_id', 'categories': 'course_id', })

        courseData = courseData.assign(
            course_id=courseData['course_id'].str.split(',')).explode('course_id')

        courseData['in_context'] = [
            c.endswith('>InContext') for c in courseData['course_id']]

        courseData['course_id'] = courseData['course_id'].str.replace(
            r'^' + categoryFilter + r'.*>([0-9]+).*$',
            lambda m: m.groups()[0], regex=True
        )

        # find and drop non-decimal course IDs
        # (e.g., like category "Canvas_UMich>site>channels>Shared Repository")
        badCourseIds = courseData[courseData['course_id'].str.contains('[^0-9]', regex=True)]
        if (not badCourseIds.empty):
            logger.debug(f'Non-numeric course IDs to be removed:\n{badCourseIds}')

        courseData = courseData[courseData['course_id'].str.isdecimal()].drop_duplicates()

        return courseData

    @staticmethod
    def _makeCreationData(resultDictionaries) -> pd.DataFrame:
        creationData: pd.DataFrame = pd.DataFrame.from_records(
            resultDictionaries, columns=('id', 'createdAt', 'name', 'duration',)
        ).rename(columns={'createdAt': 'created_at'})

        creationData['created_at'] = pd.to_datetime(creationData['created_at'], unit='s')

        return creationData

    def run(self) -> Sequence[DataSourceStatus]:
        '''
        The main controller that runs each method required to update the data.

        :return: List of DataSourceStatus
        '''
        return [
            self.mediaStartedHourly(),
            self.mediaCreation(),
        ]


def main() -> Sequence[DataSourceStatus]:
    '''
    This method is invoked when its module is executed as a standalone program.

    :return: List of DataSourceStatus
    '''
    return MiVideoExtract().run()


if '__main__' == __name__:
    main()
