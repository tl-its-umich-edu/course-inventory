#
# file: migrations/0005.canvas_usage_table.py
#
from yoyo import step

__depends__ = {'0004.add_sis_id'}
step('''
    CREATE TABLE IF NOT EXISTS canvas_usage
    (
        canvas_id INTEGER NOT NULL,
        views INTEGER NOT NULL,
        participations INTEGER NOT NULL,
        date DATE NOT NULL
    )
    ENGINE=InnoDB
    CHARACTER SET utf8mb4
    ''')