#
# file: migrations/0008.add_meta_tables.py
#
from yoyo import step

__depends__ = {'0007.add_section_table'}

step('''
    CREATE TABLE IF NOT EXISTS job_run
    (
        id INTEGER NOT NULL UNIQUE AUTO_INCREMENT,
        job_name VARCHAR(50) NOT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    )
    ENGINE=InnoDB
    CHARACTER SET utf8mb4;
''')

step('''
    CREATE TABLE IF NOT EXISTS data_source_status
    (
        id INTEGER NOT NULL UNIQUE AUTO_INCREMENT,
        data_source_name VARCHAR(50) NOT NULL,
        data_updated_at DATETIME NOT NULL,
        job_run_id INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE ON UPDATE CASCADE
    )
    ENGINE=InnoDB
    CHARACTER SET utf8mb4;
''')
