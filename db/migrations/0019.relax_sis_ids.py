#
# file: migrations/0019.relax_sis_ids.py
#
from yoyo import step


steps = [
    step('''
        ALTER TABLE course_section
        MODIFY
            sis_id VARCHAR(256) NULL;
    '''),
    step('''
        ALTER TABLE account
        MODIFY
            sis_id VARCHAR(256) NULL;
    ''')
]
