'''
Add in the initial LTI Type Table
'''

from yoyo import step

__depends__ = {'0020.add_lti_usage'}

steps = [
   step('''
        CREATE TABLE IF NOT EXISTS lti_type (
            canvas_id INTEGER NOT NULL,
            des VARCHAR(200),
            rpt_group_nm VARCHAR(200),
            PRIMARY KEY (canvas_id)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
    '''),
    step('''
        ALTER TABLE lti_placement MODIFY placement_type_id INTEGER NULL,
        ADD FOREIGN KEY (placement_type_id) REFERENCES lti_type(canvas_id) 
        ON DELETE SET NULL ON UPDATE CASCADE
    '''),
    step('''
        INSERT INTO lti_type (canvas_id, des, rpt_group_nm)
        VALUES (25194, 'Zoom', 'Zoom'),
               (4352, 'Bluejeans', 'Bluejeans');
    '''),
]
