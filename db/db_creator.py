# This is needed for type hinting with fluent interfaces
from __future__ import annotations

# standard libraries
import logging, os
from typing import Dict, List
from urllib.parse import quote_plus

# third-party libraries
from sqlalchemy.engine import create_engine, Engine
from yoyo import get_backend, read_migrations


# Initialize settings and global variables

logger = logging.getLogger(__name__)

PARENT_PATH = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_PATH = os.path.join(PARENT_PATH, 'migrations')

# The metadata tables are append tables by default.
DEFAULT_APPEND_TABLE_NAMES = ['job_run', 'data_source_status']


class DBCreator:

    def __init__(
        self,
        db_params: Dict[str, str],
        append_table_names: List[str] = []
    ) -> None:

        self.db_name: str = db_params['dbname']
        self.conn_str: str = (
            'mysql+mysqldb' +
            f"://{db_params['user']}" +
            f":{quote_plus(db_params['password'])}" +
            f"@{db_params['host']}" +
            f":{db_params['port']}" +
            f"/{db_params['dbname']}?charset=utf8&ssl=true"
        )
        self.engine: Engine = create_engine(self.conn_str)

        self.append_table_names: List[str] = append_table_names
        self.append_table_names += DEFAULT_APPEND_TABLE_NAMES

    def get_table_names(self) -> List[str]:
        logger.debug('get_table_names')
        return self.engine.table_names()

    def migrate(self) -> DBCreator:
        logger.debug('migrate')
        backend = get_backend(self.conn_str)
        migrations = read_migrations(MIGRATIONS_PATH)
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))
        return self

    def drop_records(self) -> DBCreator:
        logger.debug('drop_records')
        conn = self.engine.connect()
        conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        for table_name in self.get_table_names():
            if 'yoyo' not in table_name and table_name not in self.append_table_names:
                logger.debug(f'Table Name: {table_name}')
                conn.execute(f'DELETE FROM {table_name};')
                logger.info(f'Dropped records in {table_name} in {self.db_name}')
        conn.execute('SET FOREIGN_KEY_CHECKS=1;')
        return self

    def set_up_database(self) -> DBCreator:
        self.drop_records().migrate()
        return self
