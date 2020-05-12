#
# file: migrations/0015.add_account_table.py
#
from yoyo import step

__depends__ = {'0005.change_pks_to_canvas_id'}

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS account
        (
            canvas_id INTEGER NOT NULL UNIQUE,
            sis_id INTEGER NULL UNIQUE,
            name VARCHAR(100) NOT NULL UNIQUE,
            PRIMARY KEY (canvas_id)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
    '''),
    step('''
        ALTER TABLE course
        ADD CONSTRAINT fk_account_id
            FOREIGN KEY (account_id)
            REFERENCES account(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
    ''')
]