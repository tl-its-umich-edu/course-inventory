#
# file: migrations/0005.change_pks_to_canvas_id.py
#
from yoyo import step

__depends__ = {'0004.add_sis_id'}

steps = [
    # Remove foreign key constraints
    step('''
        ALTER TABLE enrollment
        DROP FOREIGN KEY enrollment_ibfk_1
    '''),
    step('''
        ALTER TABLE enrollment
        DROP FOREIGN KEY enrollment_ibfk_2
    '''),
    # Change data types to match future foreign keys
    step('''
        ALTER TABLE enrollment
        MODIFY
            course_id INTEGER NOT NULL;
    '''),
    step('''
        ALTER TABLE enrollment
        MODIFY
            user_id INTEGER NOT NULL;
    '''),
    step('''
        ALTER TABLE enrollment
        MODIFY
            course_section_id INTEGER NOT NULL;
    '''),
    # Drop warehouse_id columns, add canvas_id primary keys
    step('''
        ALTER TABLE course
        DROP COLUMN warehouse_id;
    '''),
    step('''
        ALTER TABLE course
        ADD PRIMARY KEY(canvas_id);
    '''),
    step('''
        ALTER TABLE user
        DROP COLUMN warehouse_id;
    '''),
    step('''
        ALTER TABLE user
        ADD PRIMARY KEY(canvas_id);
    '''),
    step('''
        ALTER TABLE enrollment
        DROP COLUMN warehouse_id;
    '''),
    step('''
        ALTER TABLE enrollment
        ADD PRIMARY KEY(canvas_id);
    '''),
    # Re-establish foreign-key relationships
    step('''
        ALTER TABLE enrollment
        ADD CONSTRAINT fk_course_id
            FOREIGN KEY (course_id)
            REFERENCES course(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
    '''),
    step('''
        ALTER TABLE enrollment
        ADD CONSTRAINT fk_user_id
            FOREIGN KEY (user_id)
            REFERENCES user(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
    ''')
]
