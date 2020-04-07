from yoyo import step

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_started_hourly 
        (
            id BIGINT auto_increment NOT NULL UNIQUE,
            event_hour_utc varchar(20) NOT NULL,
            course_id BIGINT NOT NULL,
            event_time_utc_latest TIMESTAMP NOT NULL,
            event_count BIGINT NOT NULL,
            PRIMARY KEY (id)
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4;
    '''),
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_creation 
        (
            id BIGINT auto_increment NOT NULL UNIQUE,
            media_id varchar(200) NOT NULL,
            media_time_created_utc TIMESTAMP NOT NULL,
            media_name varchar(200) NOT NULL,
            media_duration_seconds BIGINT NOT NULL,
            course_id BIGINT NOT NULL,
            event_time_utc_latest TIMESTAMP NOT NULL,
            PRIMARY KEY (id)
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4;
    '''),
]
