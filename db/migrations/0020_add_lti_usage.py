'''
Migration for LTI Usage tables
'''

from yoyo import step

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS lti_placement (
            id BIGINT NOT NULL,
            course_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            course_name VARCHAR(200),
            placement_type_id INTEGER NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
    step('''
        CREATE TABLE IF NOT EXISTS lti_zoom_meeting (
            id BIGINT NOT NULL,
            lti_placement_id BIGINT NOT NULL,
            meeting_id VARCHAR(100) NOT NULL,
            host_id VARCHAR(100) NOT NULL,
            start_time DATETIME NOT NULL,
            status INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY (lti_placement_id) REFERENCES lti_placement(id) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
]
