#
# file: migrations/0008.canvas_usage_table
#
from yoyo import step

__depends__ = {'0007.add_section_table'}

step('''
        CREATE TABLE IF NOT EXISTS canvas_course_usage(
            id BIGINT NOT NULL AUTO_INCREMENT,
            course_id INTEGER NOT NULL,
            views INTEGER NOT NULL,
            participations INTEGER NOT NULL,
            date DATE NOT NULL,
            PRIMARY KEY (id)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
''')

