#
# file: migrations/0023.relax_course_name.py
#
from yoyo import step

steps = [
    step('''
        ALTER TABLE course
        DROP INDEX name;
    '''),
    step('''
        ALTER TABLE course
        MODIFY
            name VARCHAR(256) NOT NULL;
    ''')
]
