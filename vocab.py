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

    def __str__(self) -> str:
        '''
        Return value name without `ValidDataSourceName` class name.

        :return: Name of enum value.
        '''
        return str(self.name)



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
        Create a data source status object with name and update timestamp.

        :param dataSourceName: `ValidDataSourceName` - Name of data source
        :param dataUpdatedAt: `datetime` - Time when data from the source was updated
        '''
        self._dataSourceName: ValidDataSourceName
        self._dataUpdatedAt: datetime
        self.setDataSourceName(dataSourceName).setDataUpdatedAt(dataUpdatedAt)

    @property
    def dataSourceName(self) -> ValidDataSourceName:
        '''Name of the data source'''
        return self._dataSourceName

    @dataSourceName.setter
    def dataSourceName(self, dataSourceName: ValidDataSourceName = NotImplemented):
        self.setDataSourceName(dataSourceName)

    @property
    def dataUpdatedAt(self) -> datetime:
        '''Time when data from the source was updated'''
        return self._dataUpdatedAt

    @dataUpdatedAt.setter
    def dataUpdatedAt(self, dataUpdatedAt: datetime):
        self.setDataUpdatedAt(dataUpdatedAt)

    def setDataSourceName(
            self,
            dataSourceName: ValidDataSourceName = NotImplemented
    ) -> DataSourceStatus:
        '''
        Set name of data source.  Main logic and fluent interface.

        :param dataSourceName: `ValidDataSourceName` - Name of data source
        :return: `DataSourceStatus`
        '''
        if (dataSourceName is NotImplemented):
            raise ValueError('dataSourceName must be specified')
        if (not isinstance(dataSourceName, ValidDataSourceName)):
            raise ValueError('dataSourceName must be of type ValidDataSourceName')

        self._dataSourceName = dataSourceName
        return self

    def setDataUpdatedAt(self, dataUpdatedAt: datetime = NotImplemented) -> DataSourceStatus:
        '''
        Set time data source updated.  Main logic and fluent interface.

        :param dataUpdatedAt: `datetime` - Time data source updated
        :return: `DataSourceStatus`
        '''
        if (dataUpdatedAt is NotImplemented):
            self._dataUpdatedAt = pd.to_datetime(time.time(), unit='s', utc=True)
        elif (dataUpdatedAt.tzinfo is not UTC):
            raise ValueError('_dataUpdatedAt must have UTC time zone')
        else:
            self._dataUpdatedAt = dataUpdatedAt
        return self

    def copy(self) -> Dict:
        '''
        Return dictionary representation of DataSourceStatus object.

        :return: `Dict` of object properties
        '''

        return {
            'data_source_name': self._dataSourceName,
            'data_updated_at': self._dataUpdatedAt
        }
