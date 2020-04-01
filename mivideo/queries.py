# -*- coding: utf-8 -*-

def dfCourseEventsQuery(lastTime: str) -> str:
    return f'''
        SELECT
        DISTINCT event_hour_utc,
                 course_id,
                 MAX(event_time) AS event_time_utc_latest,
                 COUNT(*) AS event_count
        FROM
            (
            SELECT
        SAFE_CAST(
            JSON_EXTRACT_SCALAR(
                event,
                '$.object.extensions.kaf:course_id'
            ) AS INT64
        )AS course_id,
            FORMAT_TIMESTAMP(
                '%F %H',
                event_time,
                'UTC'
            ) AS event_hour_utc,
                 event_time
        FROM
        `udp-umich-prod`.event_store.events
        WHERE
            (
            ed_app = 'https://aakaf.mivideo.it.umich.edu/caliper/info/app/KafEdApp'
        OR ed_app = 'https://1038472-1.kaf.kaltura.com/caliper/info/app/KafEdApp'
        )
        AND event_time > TIMESTAMP('{lastTime}')
        AND event_time < TIMESTAMP(
            CURRENT_DATE()
        )
        AND TYPE = 'MediaEvent'
        AND ACTION = 'Started'
        )
        WHERE
        course_id IS NOT NULL
        GROUP BY
        event_hour_utc,
        course_id
        ORDER BY
        event_time_utc_latest
'''


def dfMivideoCreationQuery(lastTime: str) -> str:
    '''
    The results of this probably contains duplicates of results returned the previous days.
    Solutions:
        1. Replace this query with requests to the Kaltura API (in progress).
        2. Load all previous data into a dataframe to find the last event time and the last
            media creation time.  Use the last event time to reduce the scope of the query and
            the last media creation time to find any new media.

        Either of these solutions may skip some data which relates media to courses if existing
        media is added to a course and there are no events for the media from that course.


    :param lastTime: valid standard SQL timestamp
    :return: SQL query
    '''


    return f'''
        SELECT
          DISTINCT media_id,
          time_created_utc,
          media_name,
          SAFE_CAST(
            substr(
              media_duration,
              3,
              LENGTH(media_duration) - 3
            ) AS INT64
          ) AS media_duration_seconds,
          course_id,
          MAX(event_time) AS event_time_utc_latest
        FROM
          (
            SELECT
              SAFE_CAST(
                JSON_EXTRACT_SCALAR(
                  event,
                  '$.object.extensions.kaf:course_id'
                ) AS INT64
              )AS course_id,
              TIMESTAMP(
                JSON_EXTRACT_SCALAR(
                  event,
                  '$.object.dateCreated'
                )
              ) AS time_created_utc,
              JSON_EXTRACT_SCALAR(
                event,
                '$.object.id'
              ) AS media_id,
              JSON_EXTRACT_SCALAR(
                event,
                '$.object.name'
              ) AS media_name,
              JSON_EXTRACT_SCALAR(
                event,
                '$.object.duration'
              ) AS media_duration,
              event_time
            FROM
              `udp-umich-prod`.event_store.events
            WHERE
              (
                ed_app = 'https://aakaf.mivideo.it.umich.edu/caliper/info/app/KafEdApp'
                OR ed_app = 'https://1038472-1.kaf.kaltura.com/caliper/info/app/KafEdApp'
              )
              AND event_time > TIMESTAMP('{lastTime}')
              AND event_time < TIMESTAMP(
                CURRENT_DATE()
              )
              AND TYPE = 'MediaEvent'
              AND ACTION = 'Started'
          )
        WHERE
          course_id IS NOT NULL
        GROUP BY
          media_id,
          time_created_utc,
          media_name,
          media_duration,
          course_id
        ORDER BY
          event_time_utc_latest
'''
