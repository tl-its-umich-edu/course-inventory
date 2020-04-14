# standard libraries
from enum import auto, Enum


# Enum(s)

class ValidJobName(Enum):
    """
    Each job name should be defined in ValidJobName.
    NAME_OF_JOB = 'path.to.method'
    """
    
    COURSE_INVENTORY = 'course_inventory.inventory.run_course_inventory'
    # ONLINE_MEETINGS = 'online_meetings.report...'
    # ZOOM = 'online_meetings.canvas_zoom_meetings...'
    # MIVIDEO = 'mivideo...'


class ValidDataSourceName(Enum):
    """
    Each data source name should be defined in ValidDataSourceName.
    NAME_OF_DATA_SOURCE = auto()
    """
    
    CANVAS_API = auto()
    UNIZIN_DATA_WAREHOUSE = auto()
