#
# file: migrations/0011.add_course_sis_ids.py
#
from yoyo import step

__depends__ = {'0010.add_fkc_course_usage_table'}


steps = [
    step('''
        ALTER TABLE course
        ADD COLUMN sis_id BIGINT NULL AFTER canvas_id;
    '''),
    step('''
        ALTER TABLE course_section
        ADD COLUMN sis_id BIGINT NULL AFTER canvas_id;
    ''')
]
