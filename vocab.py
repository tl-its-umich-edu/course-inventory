from __future__ import annotations

# standard libraries
import time
from datetime import datetime
from enum import auto, Enum
from typing import Dict, Union

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
    UNIZIN_DATA_PLATFORM_EVENTS = auto()
    UNIZIN_DATA_WAREHOUSE = auto()
    CANVAS_ZOOM_MEETINGS = auto()


class DataSourceTimestamp:
    def __init__(
            self,
            dataSource: ValidDataSourceName = NotImplemented,
            timestamp: datetime = NotImplemented
    ) -> None:
        self.dataSource: ValidDataSourceName
        self.timestamp: datetime
        self.setDataSource(dataSource)
        self.setTimestamp(timestamp)

    def setDataSource(
            self,
            dataSource: ValidDataSourceName = NotImplemented
    ) -> DataSourceTimestamp:
        if (dataSource is NotImplemented):
            raise ValueError('dataSource must be specified')
        self.dataSource = dataSource
        return self

    def getDataSource(self) -> ValidDataSourceName:
        return self.dataSource

    def setTimestamp(self, timestamp: datetime = NotImplemented) -> DataSourceTimestamp:
        if (timestamp is NotImplemented):
            self.timestamp = pd.to_datetime(time.time(), unit='s', utc=True)
        elif (timestamp.tzinfo is not UTC):
            raise ValueError('timestamp must have UTC time zone')
        else:
            self.timestamp = timestamp
        return self

    def getTimestamp(self) -> datetime:
        return self.timestamp

    def copy(self) -> Dict:
        '''
        For backwards compatibility with code that expects to copy this object as a dictionary.

        TODO: Update other code to not need this, then remove this method.

        :return: Dict of object properties
        '''
        return {
            'data_source_name': self.dataSource,
            'data_updated_at': self.timestamp
        }

    def __getitem__(self, item: str) -> Union[ValidDataSourceName, datetime]:
        '''
        For backwards compatibility with code that expects to use this object as a dictionary.

        TODO: Update other code to not need this, then remove this method.

        :param item: Name of the property to get, "data_source_name" or "data_updated_at".
        :return:
        '''
        return self.copy()[item]
