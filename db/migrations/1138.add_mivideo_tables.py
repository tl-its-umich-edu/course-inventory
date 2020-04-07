from yoyo import step

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
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_creation (
            id BIGINT AUTO_INCREMENT NOT NULL UNIQUE,
            media_id VARCHAR(200) NOT NULL,
            media_time_created_utc DATETIME NOT NULL,
            media_name VARCHAR(200) NOT NULL,
            media_duration_seconds BIGINT NOT NULL,
            course_id BIGINT NOT NULL,
            event_time_utc_latest DATETIME NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
]
