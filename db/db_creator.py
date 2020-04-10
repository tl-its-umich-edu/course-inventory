# This is needed for type hinting with fluent interfaces
from __future__ import annotations

# standard libraries
import logging, os
from typing import Dict, List, Sequence, Union
from urllib.parse import quote_plus

# third-party libraries
from sqlalchemy.engine import create_engine, Connection, Engine
from yoyo import get_backend, read_migrations

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_PATH = os.path.join(ROOT_DIR, 'migrations')

# The metadata tables are append tables by default.
DEFAULT_APPEND_TABLES = ['job_run', 'data_source_status']


class DBCreator:

    def __init__(
        self,
        db_params: Dict[str, str],
        append_table_names: List[str] = []
    ) -> None:

        self.db_name: str = db_params['dbname']
        self.conn: Union[Connection, None] = None
        self.conn_str: str = (
            'mysql+mysqldb' +
            f"://{db_params['user']}" +
            f":{quote_plus(db_params['password'])}" +
            f"@{db_params['host']}" +
            f":{db_params['port']}" +
            f"/{db_params['dbname']}?charset=utf8&ssl=true"
        )
        self.engine: Engine = create_engine(self.conn_str)
        self.append_table_names: List[str] = append_table_names + DEFAULT_APPEND_TABLES

    def connect(self) -> DBCreator:
        logger.debug('set_up')
        self.conn = self.engine.connect()
        return self

    def close(self) -> DBCreator:
        logger.debug('tear_down')
        if not isinstance(self.conn, Connection):
            logger.error('self.conn needs to be initalized before it can be closed')
        else:
            self.conn.close()
        return self

    def get_table_names(self) -> Sequence[str]:
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
        if not isinstance(self.conn, Connection):
            logger.error('self.conn needs to be initalized first before it can be used')
        else:
            self.conn.execute('SET FOREIGN_KEY_CHECKS=0;')
            for table_name in self.get_table_names():
                if 'yoyo' not in table_name and table_name not in self.append_table_names:
                    logger.debug(f'Table Name: {table_name}')
                    self.conn.execute(f'DELETE FROM {table_name};')
                    logger.info(f'Dropped records in {table_name} in {self.db_name}')
            self.conn.execute('SET FOREIGN_KEY_CHECKS=1;')
        return self

    def set_up_database(self) -> DBCreator:
        self.connect().drop_records().close().migrate()
        return self
