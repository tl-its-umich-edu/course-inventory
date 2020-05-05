# -*- coding: utf-8 -*-
'''
SQL queries for MiVideo data extraction
'''

COURSE_EVENTS: str = '''
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
          AND event_time > TIMESTAMP(@startTime)
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
      event_time_utc_latest,
      course_id
'''
