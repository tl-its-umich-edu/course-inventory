# -*- coding: utf-8 -*-
import csv
from io import StringIO
from os import path
from typing import Union

import pandas
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

import queries

CSV_OPTIONS: dict = {
    'index': False,
    'quoting': csv.QUOTE_NONNUMERIC,
    'mode': 'a',  # append to end of file
}
KEY_PATH: str = '/Users/lsloan/Documents/UDP Service Account/umich-lsloan-prod.json'
SHAPE_COLUMNS: int = 0  # Index of column count in DataFrame.shape array

lastTimeOverride: str = sys.argv[1] if (len(sys.argv) > 1) else None


def readFileLastLine(filename: str) -> Union[str, None]:
    lastLine: str = None

    try:
        with open(filename, 'r') as f:
            lastLine = f.readlines()[-1]
    except:
        pass

    return lastLine


def readFileLastTime(filename: str, timeColumnNumber: int) -> Union[str, None]:
    lastTime: str = None

    try:
        lastTime = (
            pandas.read_csv(StringIO(readFileLastLine(filename)), header=None)
                .iloc[0][timeColumnNumber]
        )
    except:
        pass

    # lastTime = '2020-03-01'
    return lastTime


credentials: service_account.Credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

print(f'Connected to BigQuery project: {client.project}')

"""### Hourly Count of Events by Course ID"""

filename = 'mivideo_media_started_hourly_by_course_id.csv'

print(f'"{filename}" - Starting procedure…')

lastTime = lastTimeOverride or readFileLastTime(filename, timeColumnNumber=2) or '2020-03-01'

print(f'"{filename}" - Last time found in file: "{lastTime}"')

print(f'"{filename}" - Running query…')

dfCourseEvents: pandas.DataFrame = client.query(
    queries.dfCourseEventsQuery(lastTime)).to_dataframe()

print(f'"{filename}" - Completed query.')

"""#### CSV"""

if (not dfCourseEvents.empty):
    print(f'"{filename}" - Number of rows returned: ({dfCourseEvents.shape[SHAPE_COLUMNS]})')

    includeHeaders = not path.exists(filename)

    print(f'"{filename}" - Saving to file with{"" if includeHeaders else "out"} headers…')

    dfCourseEvents.to_csv(
        filename,
        header=includeHeaders,
        **CSV_OPTIONS)

    print(f'"{filename}" - Saved.')
else:
    print(f'"{filename}" - No rows returned.')

print(f'"{filename}" - Procedure complete.')

"""
### Hourly Count of All Events
"""

sourceFilename = filename
filename = 'mivideo_media_started_hourly_all.csv'

print(f'"{filename}" - Starting procedure…')

if (not dfCourseEvents.empty):
    print(f'"{filename}" - Calculating new data from "{sourceFilename}"…')

    dfAllEvents: pandas.DataFrame = (
        (dfCourseEvents.drop(columns='course_id').groupby(['event_hour_utc']).sum())
            .sort_values(by='event_hour_utc')
            .reset_index())

    print(f'"{filename}" - Data calculated.')

    """#### CSV"""

    print(f'"{filename}" - Number of rows calculated: ({dfAllEvents.shape[SHAPE_COLUMNS]})')

    includeHeaders = not path.exists(filename)

    print(f'"{filename}" - Saving to file with{"" if includeHeaders else "out"} headers…')

    dfAllEvents.to_csv(
        filename,
        header=includeHeaders,
        **CSV_OPTIONS)
else:
    print(f'"{filename}" - Not updating.  No new data from "{sourceFilename}".')

print(f'"{filename}" - Starting procedure…')
print(f'"{filename}" - Procedure complete.')

"""### MiVideo Media Creation info"""

filename = 'mivideo_media_creation.csv'

print(f'"{filename}" - Starting procedure…')

lastTime = lastTimeOverride or readFileLastTime(filename, timeColumnNumber=5) or '1970-01-01'

print(f'"{filename}" - Last time found in file: "{lastTime}"')

print(f'"{filename}" - Running query…')

dfMivideoCreation: pandas.DataFrame = client.query(queries.dfMivideoCreationQuery(lastTime)).to_dataframe()

print(f'"{filename}" - Completed query.')


"""#### CSV"""

if (not dfMivideoCreation.empty):
    print(f'"{filename}" - Number of rows returned: ({dfMivideoCreation.shape[SHAPE_COLUMNS]})')

    includeHeaders = not path.exists(filename)

    print(f'"{filename}" - Saving to file with{"" if includeHeaders else "out"} headers…')

    dfMivideoCreation.to_csv('mivideo_media_creation.csv', **CSV_OPTIONS)
else:
    print(f'"{filename}" - No rows returned.')

print(f'"{filename}" - Procedure complete.')

print('🏁 End of Report')
