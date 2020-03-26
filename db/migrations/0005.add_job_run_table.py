#
# file: migrations/0005.add_job_run_table.py
#
from yoyo import step

__depends__ = {'0004.add_sis_id'}

step('''
    CREATE TABLE IF NOT EXISTS job_run
    (
        id INTEGER NOT NULL UNIQUE AUTO_INCREMENT,
        finished_at DATETIME NOT NULL,
        unizin_data_updated_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    ENGINE=InnoDB
    CHARACTER SET utf8mb4;
''')
