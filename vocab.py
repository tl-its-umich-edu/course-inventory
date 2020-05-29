from __future__ import annotations

# standard libraries
import time
from datetime import datetime
from enum import auto, Enum
from typing import Dict, Union

import pytz


# Enum(s)
class ValidJobName(Enum):
    """
    Each job name should be defined in ValidJobName.
    NAME_OF_JOB = 'path.to.method'
    """

    COURSE_INVENTORY = 'course_inventory.inventory.run_course_inventory'
    MIVIDEO = 'mivideo.mivideo_extract.main'
    CANVAS_LTI = 'lti_placements.canvas_placements.main'


class ValidDataSourceName(Enum):
    """
    Each data source name should be defined in ValidDataSourceName.
    NAME_OF_DATA_SOURCE = auto()
    """

    CANVAS_API = auto()
    CANVAS_LTI = auto()
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
            data_source_name: ValidDataSourceName = None,
            data_updated_at: datetime = None
    ) -> None:
        '''
        Create a data source status object with name and update timestamp.

        :param data_source_name: `ValidDataSourceName` - Name of data source
        :param data_updated_at: `datetime` - Time when data from the source was updated
        '''
        self._data_source_name: ValidDataSourceName
        self._data_updated_at: datetime
        self.set_data_source_name(data_source_name).set_data_updated_at(data_updated_at)

    @property
    def data_source_name(self) -> ValidDataSourceName:
        '''Name of the data source'''
        return self._data_source_name

    @data_source_name.setter
    def data_source_name(self, data_source_name: ValidDataSourceName = None):
        self.set_data_source_name(data_source_name)

    @property
    def data_updated_at(self) -> datetime:
        '''Time when data from the source was updated'''
        return self._data_updated_at

    @data_updated_at.setter
    def data_updated_at(self, data_updated_at: datetime):
        self.set_data_updated_at(data_updated_at)

    def set_data_source_name(
            self,
            data_source_name: ValidDataSourceName = None
    ) -> DataSourceStatus:
        '''
        Set name of data source.  Main logic and fluent interface.

        :param data_source_name: `ValidDataSourceName` - Name of data source
        :return: `DataSourceStatus`
        '''
        if (data_source_name is None):
            raise ValueError('data_source_name must be specified')
        if (not isinstance(data_source_name, ValidDataSourceName)):
            raise ValueError('data_source_name must be of type ValidDataSourceName')

        self._data_source_name = data_source_name
        return self

    def set_data_updated_at(self, data_updated_at: datetime = None) -> DataSourceStatus:
        '''
        Set time data source updated.  Main logic and fluent interface.

        :param data_updated_at: `datetime` - Time data source updated
        :return: `DataSourceStatus`
        '''
        if (data_updated_at is None):
            self._data_updated_at = datetime.fromtimestamp(time.time(), pytz.UTC)
        elif (type(data_updated_at) is not datetime):  # pylint: disable=unidiomatic-typecheck
            # Prevent use of pandas.Timestamp, which is incompatible with SQLAlchemy.
            # Note: This kind of type check may be unpythonic because it defeats duck typing.
            raise TypeError('data_updated_at must be of type datetime')
        elif (data_updated_at.tzinfo is not pytz.UTC):
            raise ValueError('data_updated_at must have UTC time zone')
        else:
            self._data_updated_at = data_updated_at
        return self

    def copy(self) -> Dict[str, Union[ValidDataSourceName, datetime]]:
        '''
        Return dictionary representation of DataSourceStatus object.

        :return: `Dict` of object properties
        '''

        return {
            'data_source_name': self._data_source_name,
            'data_updated_at': self._data_updated_at
        }
