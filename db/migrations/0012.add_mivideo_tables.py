from yoyo import step

__depends__ = {'0009.add_meta_tables.py'}

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_started_hourly (
            id BIGINT AUTO_INCREMENT NOT NULL UNIQUE,
            event_hour_utc VARCHAR(20) NOT NULL,
            course_id BIGINT NOT NULL,
            event_time_utc_latest DATETIME NOT NULL,
            event_count BIGINT NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
]
