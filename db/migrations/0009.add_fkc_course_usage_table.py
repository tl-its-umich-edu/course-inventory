#
# file: migrations/0009.add_fkc_course_usage_table.py
#
from yoyo import step

__depends__ = {'0008.canvas_usage_table'}

step('''
        ALTER TABLE canvas_course_usage
        ADD CONSTRAINT fk_canvas_course_usage_id
            FOREIGN KEY (course_id)
            REFERENCES course(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
''')
