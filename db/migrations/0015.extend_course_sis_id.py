#
# file: migrations/0015.extend_course_sis_id.py
#
from yoyo import step

step('''
    ALTER TABLE course
    MODIFY
        sis_id VARCHAR(256) NULL;
''')
