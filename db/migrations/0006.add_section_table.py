#
# file: migrations/0006.add_section_table.py
#
from yoyo import step

__depends__ = {'0005.modify_user'}

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS course_section
        (
            warehouse_id BIGINT NOT NULL UNIQUE,
            canvas_id INTEGER NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL,
            PRIMARY KEY (warehouse_id)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
    '''),
    step('''
        ALTER TABLE enrollment
        ADD CONSTRAINT fk_course_section_id
            FOREIGN KEY (course_section_id)
            REFERENCES course_section(warehouse_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
    ''')
]
