# This is needed for type hinting with fluent interfaces
from __future__ import annotations

# standard libraries
import logging, os
from typing import Dict, List, Sequence, Union
from urllib.parse import quote_plus

# third-party libraries
from sqlalchemy.engine import create_engine, Engine
from yoyo import get_backend, read_migrations


# Initialize settings and global variables

logger = logging.getLogger(__name__)

PARENT_PATH = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_PATH = os.path.join(PARENT_PATH, 'migrations')


class DBCreator:
    '''
    Utility class for managing the application's database. Leverages SQLAlchemy
    and yoyo-migrations. The migrate, drop_records, and reset_database methods can be
    used fluently, i.e. with method chaining (see reset_database for an example).
    '''

    def __init__(self, db_params: Dict[str, str]) -> None:
        '''
        Sets the database name; sets the connection string; uses the connection string
        to create a SQLAlchemy engine object.
        '''
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

    def get_table_names(self) -> List[str]:
        '''
        Gets table names using the SQLAlchemy Engine object.
        '''
        logger.debug('get_table_names')
        return self.engine.table_names()

    def migrate(self) -> DBCreator:
        '''
        Updates database schema using yoyo-migrations and the migration files in the
        migrations directory collocated with this file (db_creator.py).
        '''
        logger.debug('migrate')
        backend = get_backend(self.conn_str)
        migrations = read_migrations(MIGRATIONS_PATH)
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))
        return self

    def drop_records(self, spec_table_names: Union[Sequence[str], None] = None) -> DBCreator:
        '''
        Drops records from either the specified database tables or all of the
        application-managed database tables; tables managed by yoyo-migrations are ignored.
        '''
        logger.debug('drop_records')
        app_table_names = [
            table_name for table_name in self.get_table_names() if 'yoyo' not in table_name
        ]
        logger.debug(f'app_table_names: {app_table_names}')

        if spec_table_names is None:
            # Drop all non-yoyo tables
            logger.info('Dropping all application (non-yoyo) tables')
            drop_table_names = app_table_names
        else:
            # Drop specified table names if they're valid (i.e. an application-managed table)
            logger.info('Dropping specific provided tables if they are valid')
            drop_table_names = []
            for spec_table_name in spec_table_names:
                if spec_table_name in app_table_names:
                    drop_table_names.append(spec_table_name)
                else:
                    logger.error(f'Invalid table name was provided: {spec_table_name}')

        conn = self.engine.connect()
        conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        for drop_table_name in drop_table_names:
            logger.debug(f'Table Name: {drop_table_name}')
            conn.execute(f'DELETE FROM {drop_table_name};')
            logger.info(f'Dropped records in {drop_table_name} in {self.db_name}')
        conn.execute('SET FOREIGN_KEY_CHECKS=1;')
        return self

    def reset_database(self) -> DBCreator:
        '''
        Drops records in application-managed tables and applies outstanding migrations
        '''
        self.drop_records().migrate()
        return self

    def get_pk_values(self, table_name: str, primary_key: str) -> List[Union[int, None]]:
        '''
        Retrieves primary key values from the table. Only works with one primary key.
        '''
        pk_values = []
        conn = self.engine.connect()
        rs = conn.execute(f"SELECT {primary_key} FROM {table_name}")
        for row in rs:
            pk_values.append(row[0])
        return pk_values
