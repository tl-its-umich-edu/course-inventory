'''
Migration for MiVideo media started hourly table
'''

from yoyo import step

__depends__ = {'0012.add_mivideo_usage_table'}


steps = [
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_created (
            id VARCHAR(20) NOT NULL UNIQUE,
            created_at DATETIME NOT NULL,
            name VARCHAR(200) NOT NULL,
            duration BIGINT NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
            # id BIGINT AUTO_INCREMENT NOT NULL UNIQUE,
    step('''
        CREATE TABLE IF NOT EXISTS mivideo_media_courses (
            media_id VARCHAR(20) NOT NULL UNIQUE,
            course_id INTEGER NOT NULL,
            PRIMARY KEY (media_id, course_id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
]
