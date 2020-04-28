#
# file: migrations/0013.add_term_table.py
#
from yoyo import step

steps = [
    step('''
        CREATE TABLE IF NOT EXISTS term
        (
            canvas_id INTEGER NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL UNIQUE,
            sis_id INTEGER NOT NULL,
            start_at DATETIME NOT NULL,
            end_at DATETIME NOT NULL,
            PRIMARY KEY (canvas_id)
        )
        ENGINE=InnoDB
        CHARACTER SET utf8mb4;
    '''),
    step('''
        ALTER TABLE course
        ADD COLUMN term_id INTEGER NOT NULL AFTER account_id;
    '''),
    step('''
        ALTER TABLE course
        ADD CONSTRAINT fk_term_id
            FOREIGN KEY (term_id)
            REFERENCES term(canvas_id)
            ON UPDATE CASCADE ON DELETE CASCADE;
    ''')
]
