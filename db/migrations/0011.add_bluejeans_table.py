#
# file: migrations/0011.add_bluejeans_table.py
#
from yoyo import step

__depends__ = {'0010.add_fkc_course_usage_table'}
steps = [
    step('''
        CREATE TABLE IF NOT EXISTS bluejeans_meetings
        (
            meeting_uuid varchar(255) NOT NULL UNIQUE,
            meetingTitle VARCHAR(255) NULL,
            meetingId varchar(50) NOT NULL,
            userName VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            user_tags  VARCHAR(255),
            startTime DATETIME,
            endTime DATETIME,
            email VARCHAR(255),
            participantSeconds int,
            joinDate DATETIME,
            joinWeek DATETIME,
            joinMonth VARCHAR(50),
            participants int,
            participantMinutes float,
            meetingDurationMinutes float,
            popId int,
            userType VARCHAR(255),
            billable bool,
            moderatorLess bool,
            start_time DATETIME,
            total_highlights_created int,
            smart_meeting bool,
            transcription_used bool,
            total_transcription_duration_minutes float,

            PRIMARY KEY (meeting_uuid)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
    ''')
]
