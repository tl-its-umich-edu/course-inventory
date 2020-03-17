# standard libraries
import logging
from typing import Dict, Sequence

# third-party libraries
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# Class(es)

class DBCreator:

    def __init__(self, db_name: str, engine: Engine, table_dicts: Sequence[Dict[str, str]]) -> None:
        self.conn = None
        self.db_name = db_name
        self.engine = engine
        self.tables = table_dicts

    def set_up(self) -> None:
        logger.debug('set_up')
        self.conn = self.engine.connect()

    def tear_down(self) -> None:
        logger.debug('tear_down')
        self.conn.close()

    def drop_tables(self) -> None:
        logger.debug('drop_tables')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        table_names = [table['name'] for table in self.tables]
        drop_statement = f'DROP TABLE IF EXISTS {", ".join(table_names)};'
        self.conn.execute(drop_statement)
        self.conn.execute('SET FOREIGN_KEY_CHECKS=1;')

    def drop_records(self) -> None:
        logger.debug('drop_records')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        for table_dict in self.tables:
            table_name = table_dict['name']
            logger.debug(f'Table Name: {table_name}')
            self.conn.execute(f'DELETE FROM {table_name};')
            logger.info(f'Dropped records in {table_name} in {self.db_name}')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=1;')

    def create_tables(self) -> None:
        logger.debug('create_tables')
        for table_dict in self.tables:
            table_name = table_dict['name']
            logger.debug(f'Table Name: {table_name}')
            self.conn.execute(table_dict['statement'])
            logger.info(f'Created table {table_name} in {self.db_name}')

    def set_up_database(self) -> None:
        self.set_up()
        self.drop_tables()
        self.create_tables()
        self.tear_down()
