#
# file: migrations/0010.add_fkc_course_usage_table.py
#
from yoyo import step

__depends__ = {'0009.add_meta_tables'}

step('''
        ALTER TABLE canvas_course_usage
        ADD CONSTRAINT fk_canvas_course_usage_id
            FOREIGN KEY (course_id)
            REFERENCES course(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
''')
