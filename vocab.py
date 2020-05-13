from __future__ import annotations

# standard libraries
import time
from datetime import datetime
from enum import auto, Enum
from typing import Dict

import pandas as pd
from pytz import UTC


# Enum(s)

class ValidJobName(Enum):
    """
    Each job name should be defined in ValidJobName.
    NAME_OF_JOB = 'path.to.method'
    """

    COURSE_INVENTORY = 'course_inventory.inventory.run_course_inventory'
    MIVIDEO = 'mivideo.mivideo_extract.main'
    CANVAS_ZOOM_MEETINGS = 'online_meetings.canvas_zoom_meetings.main'


class ValidDataSourceName(Enum):
    """
    Each data source name should be defined in ValidDataSourceName.
    NAME_OF_DATA_SOURCE = auto()
    """

    CANVAS_API = auto()
    CANVAS_ZOOM_MEETINGS = auto()
    KALTURA_API = auto()
    UNIZIN_DATA_PLATFORM_EVENTS = auto()
    UNIZIN_DATA_WAREHOUSE = auto()


class DataSourceStatus:
    '''
    Each data source process must return an instance of this class, which tells the name of the
    data source and the time at which data from it was last updated.
    '''
    def __init__(
            self,
            dataSourceName: ValidDataSourceName = NotImplemented,
            dataUpdatedAt: datetime = NotImplemented
    ) -> None:
        '''

        :param dataSourceName: `ValidDataSourceName` - Name of data source
        :param dataUpdatedAt: `datetime` - Time at which data from the source was updated
        '''
        self.dataSourceName: ValidDataSourceName
        self.dataUpdatedAt: datetime
        self.setDataSourceName(dataSourceName)
        self.setDataUpdatedAt(dataUpdatedAt)

    def setDataSourceName(
            self,
            dataSourceName: ValidDataSourceName = NotImplemented
    ) -> DataSourceStatus:
        '''
        Set name of data source

        :param dataSourceName: `ValidDataSourceName` - Name of data source
        :return: `DataSourceStatus`
        '''
        if (dataSourceName is NotImplemented):
            raise ValueError('dataSourceName must be specified')
        if (not isinstance(dataSourceName, ValidDataSourceName)):
            raise ValueError('dataSourceName must be of type ValidDataSourceName')

        self.dataSourceName = dataSourceName
        return self

    def getDataSourceName(self) -> ValidDataSourceName:
        '''
        Get name of data source

        :return: `ValidDataSourceName` - Name of data source
        '''
        return self.dataSourceName

    def setDataUpdatedAt(self, dataUpdatedAt: datetime = NotImplemented) -> DataSourceStatus:
        '''
        Set time data source updated

        :param dataUpdatedAt: `datetime` - Time data source updated
        :return: `DataSourceStatus`
        '''
        if (dataUpdatedAt is NotImplemented):
            self.dataUpdatedAt = pd.to_datetime(time.time(), unit='s', utc=True)
        elif (dataUpdatedAt.tzinfo is not UTC):
            raise ValueError('dataUpdatedAt must have UTC time zone')
        else:
            self.dataUpdatedAt = dataUpdatedAt
        return self

    def getDataUpdatedAt(self) -> datetime:
        '''
        Get time data source updated

        :return: `datetime` -  Time data source updated
        '''
        return self.dataUpdatedAt

    def copy(self) -> Dict:
        '''
        Typical Pythonic method to return a dictionary representation of an object.  Note that the
        `data_source_name` key contains `dataSourceName.name`, not a `ValidDataSourceName` object.

        :return: `Dict` of object properties
        '''

        return {
            'data_source_name': self.dataSourceName.name,
            'data_updated_at': self.dataUpdatedAt
        }
