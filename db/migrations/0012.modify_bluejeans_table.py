#
# file: migrations/0012.add_bluejeans_table.py
#
from yoyo import step

__depends__ = {'0011.add_course_sis_ids'}
steps = [
    step('''
        CREATE TABLE IF NOT EXISTS bluejeans_meeting
        (
            meeting_uuid VARCHAR(255) NOT NULL UNIQUE,
            meeting_title VARCHAR(255) NULL,
            meeting_id VARCHAR(50) NOT NULL,
            user_name VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            user_tags VARCHAR(255),
            start_time DATETIME,
            end_time DATETIME,
            email VARCHAR(255),
            participant_seconds INT,
            join_date DATETIME,
            join_week DATETIME,
            join_month VARCHAR(50),
            participants INT,
            participant_minutes FLOAT,
            meeting_duration_minutes FLOAT,
            pop_id INT,
            user_type VARCHAR(255),
            billable BOOL,
            moderator_less BOOL,
            total_highlights_created INT,
            smart_meeting BOOL,
            transcription_used BOOL,
            total_transcription_duration_minutes FLOAT,
            PRIMARY KEY (meeting_uuid)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
    ''')
]
