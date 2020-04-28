#
# file: migrations/0014.change_course_sis_id_data_type.py
#
from yoyo import step

step('''
    ALTER TABLE course
    MODIFY
        sis_id VARCHAR(15) NULL;
''')
